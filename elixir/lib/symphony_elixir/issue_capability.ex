defmodule SymphonyElixir.IssueCapability do
  @moduledoc """
  Verifies issue-scoped capabilities without exposing the signing key to agent children.

  The owner process supplies the 32-byte signing key through an inherited anonymous
  pipe and the active restart identity through the environment. Both inputs are
  consumed before the agent runtime starts. Issue-facing requests can then prove an
  exact issue, session, and restart without receiving Symphony's global control token.
  """

  use GenServer

  alias SymphonyElixir.ControlToken

  @fd_env "SYMPHONY_ISSUE_CAPABILITY_KEY_FD"
  @restart_env "SYMPHONY_RESTART_ID"
  @key_bytes 32
  @persistent_key {__MODULE__, :configuration}
  @restart_pattern ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
  @nonce_pattern @restart_pattern

  @type configuration :: %{key: binary(), restart_id: String.t()}
  @type fetch_result :: {:ok, configuration()} | {:error, :issue_capability_not_configured}

  @doc "Returns the owner-only descriptor name that must not reach agent children."
  @spec secret_environment_names() :: [String.t()]
  def secret_environment_names, do: [@fd_env]

  @doc "Starts the owner-side capability store."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @doc "Consumes the private startup inputs and returns a restart-stable child specification."
  @spec child_spec_from_environment(keyword()) ::
          {:ok, Supervisor.child_spec()} | {:error, atom()}
  def child_spec_from_environment(opts \\ []) do
    descriptor = System.get_env(@fd_env)
    restart_id = System.get_env(@restart_env)
    System.delete_env(@fd_env)

    prepare_child(
      load_environment(descriptor, restart_id, &ControlToken.consume_private_descriptor/2),
      opts
    )
  end

  if Mix.env() == :test do
    @doc false
    @spec child_spec_from_key_for_test(binary() | nil, String.t() | nil, keyword()) ::
            {:ok, Supervisor.child_spec()} | {:error, atom()}
    def child_spec_from_key_for_test(key, restart_id, opts \\ []) do
      configuration =
        if is_nil(key) and is_nil(restart_id),
          do: {:ok, nil},
          else: validate_configuration(key, restart_id)

      prepare_child(configuration, opts)
    end

    @doc false
    @spec child_spec_from_environment_for_test(
            String.t() | nil,
            String.t() | nil,
            (String.t(), pos_integer() -> {:ok, binary()} | {:error, atom()}),
            keyword()
          ) :: {:ok, Supervisor.child_spec()} | {:error, atom()}
    def child_spec_from_environment_for_test(descriptor, restart_id, descriptor_reader, opts \\ [])
        when is_function(descriptor_reader, 2) do
      prepare_child(load_environment(descriptor, restart_id, descriptor_reader), opts)
    end

    @doc false
    @spec replace_for_test(binary() | nil, String.t() | nil) :: :ok | {:error, atom()}
    def replace_for_test(key, restart_id) do
      configuration =
        if is_nil(key) and is_nil(restart_id),
          do: {:ok, nil},
          else: validate_configuration(key, restart_id)

      with {:ok, configuration} <- configuration do
        GenServer.call(__MODULE__, {:replace_for_test, configuration})
      end
    end

    @doc false
    @spec verify_response_for_test(
            :progress | :review,
            String.t(),
            String.t(),
            String.t(),
            map(),
            String.t()
          ) :: boolean()
    def verify_response_for_test(kind, nonce, issue_identifier, owner_session, payload, signature) do
      with {:ok, %{key: key, restart_id: restart_id}} <- fetch(),
           {:ok, message} <-
             response_message(kind, nonce, issue_identifier, owner_session, restart_id, payload),
           expected <- sign(key, message) do
        secure_equal?(signature, expected)
      else
        _ -> false
      end
    end
  end

  @doc "Returns whether issue-scoped authorization is configured for this restart."
  @spec fetch(GenServer.server()) :: fetch_result()
  def fetch(server \\ __MODULE__), do: GenServer.call(server, :fetch)

  @doc "Authenticates one nonce-bound issue request before the orchestrator consumes it atomically."
  @spec verify_request(
          :progress | :review,
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          map()
        ) :: :ok | {:error, atom()}
  def verify_request(
        kind,
        signature,
        issue_identifier,
        owner_session,
        restart_id,
        nonce,
        payload
      ) do
    verify_request_signature(
      kind,
      signature,
      issue_identifier,
      owner_session,
      restart_id,
      nonce,
      payload
    )
  end

  @doc "Signs a nonce-bound response so the owner broker can reject a listener impersonator."
  @spec sign_response(
          :progress | :review,
          String.t(),
          String.t(),
          String.t(),
          map()
        ) :: {:ok, String.t()} | {:error, atom()}
  def sign_response(kind, nonce, issue_identifier, owner_session, payload) do
    with {:ok, %{key: key, restart_id: restart_id}} <- fetch(),
         {:ok, message} <-
           response_message(kind, nonce, issue_identifier, owner_session, restart_id, payload) do
      {:ok, sign(key, message)}
    end
  end

  @impl true
  def init(opts) do
    case Keyword.get(opts, :source, :prepared) do
      :prepared ->
        {:ok, %{}}

      _ ->
        {:stop, :invalid_issue_capability_source}
    end
  end

  @impl true
  def handle_call(:fetch, _from, state), do: {:reply, stored_configuration_result(), state}

  if Mix.env() == :test do
    @impl true
    def handle_call({:replace_for_test, configuration}, _from, state) do
      store_configuration(configuration)
      {:reply, :ok, state}
    end
  end

  defp load_environment(nil, nil, _descriptor_reader), do: {:ok, stored_configuration()}

  defp load_environment(nil, restart_id, _descriptor_reader) when is_binary(restart_id) do
    case stored_configuration() do
      %{restart_id: ^restart_id} = configuration -> {:ok, configuration}
      _ -> {:error, :incomplete_issue_capability_configuration}
    end
  end

  defp load_environment(descriptor, restart_id, descriptor_reader) when is_binary(descriptor) do
    with {:ok, key} <- descriptor_reader.(descriptor, @key_bytes) do
      validate_configuration(key, restart_id)
    end
  end

  defp load_environment(_descriptor, _restart_id, _descriptor_reader),
    do: {:error, :incomplete_issue_capability_configuration}

  defp prepare_child({:ok, configuration}, opts) do
    id = Keyword.get(opts, :id, __MODULE__)
    child_opts = opts |> Keyword.delete(:id) |> Keyword.put(:source, :prepared)
    store_configuration(configuration)

    {:ok, Supervisor.child_spec({__MODULE__, child_opts}, id: id)}
  end

  defp prepare_child({:error, reason}, _opts), do: {:error, reason}

  defp validate_configuration(key, restart_id)
       when is_binary(key) and byte_size(key) == @key_bytes and is_binary(restart_id) do
    if Regex.match?(@restart_pattern, restart_id) do
      {:ok, %{key: key, restart_id: restart_id}}
    else
      {:error, :invalid_issue_capability_restart_id}
    end
  end

  defp validate_configuration(key, restart_id)
       when is_binary(key) and byte_size(key) == @key_bytes do
    if is_binary(restart_id),
      do: {:error, :invalid_issue_capability_restart_id},
      else: {:error, :incomplete_issue_capability_configuration}
  end

  defp validate_configuration(key, _restart_id) when is_binary(key),
    do: {:error, :invalid_issue_capability_key_size}

  defp validate_configuration(_key, _restart_id),
    do: {:error, :incomplete_issue_capability_configuration}

  defp store_configuration(configuration) when is_nil(configuration) or is_map(configuration) do
    :persistent_term.put(@persistent_key, configuration)
  end

  defp stored_configuration, do: :persistent_term.get(@persistent_key, nil)

  defp stored_configuration_result do
    case stored_configuration() do
      nil -> {:error, :issue_capability_not_configured}
      configuration -> {:ok, configuration}
    end
  end

  defp require_identity(issue_identifier, owner_session, restart_id)
       when is_binary(issue_identifier) and byte_size(issue_identifier) in 1..256 and
              is_binary(owner_session) and byte_size(owner_session) in 1..512 and
              is_binary(restart_id) do
    if Regex.match?(@restart_pattern, restart_id), do: :ok, else: {:error, :invalid_identity}
  end

  defp require_identity(_issue_identifier, _owner_session, _restart_id),
    do: {:error, :invalid_identity}

  defp require_nonce(nonce) when is_binary(nonce) do
    if Regex.match?(@nonce_pattern, nonce), do: :ok, else: {:error, :invalid_nonce}
  end

  defp require_nonce(_nonce), do: {:error, :invalid_nonce}

  defp verify_request_signature(
         kind,
         signature,
         issue_identifier,
         owner_session,
         restart_id,
         nonce,
         payload
       ) do
    with {:ok, %{key: key, restart_id: active_restart_id}} <- stored_configuration_result(),
         true <- restart_id == active_restart_id,
         {:ok, message} <-
           request_message(kind, nonce, issue_identifier, owner_session, restart_id, payload),
         true <- secure_equal?(signature, sign(key, message)) do
      :ok
    else
      {:error, :issue_capability_not_configured} = error -> error
      _ -> {:error, :invalid_issue_capability}
    end
  end

  defp request_message(kind, nonce, issue_identifier, owner_session, restart_id, payload)
       when kind in [:progress, :review] and is_map(payload) do
    with :ok <- require_identity(issue_identifier, owner_session, restart_id),
         :ok <- require_nonce(nonce) do
      Jason.encode([
        1,
        "request",
        Atom.to_string(kind),
        nonce,
        issue_identifier,
        owner_session,
        restart_id,
        canonical_term(payload)
      ])
    else
      _ -> {:error, :invalid_issue_capability_request}
    end
  end

  defp request_message(_kind, _nonce, _issue_identifier, _owner_session, _restart_id, _payload),
    do: {:error, :invalid_issue_capability_request}

  defp response_message(kind, nonce, issue_identifier, owner_session, restart_id, payload)
       when kind in [:progress, :review] and is_map(payload) do
    with :ok <- require_identity(issue_identifier, owner_session, restart_id),
         :ok <- require_nonce(nonce),
         {:ok, fields} <- response_fields(kind, payload) do
      Jason.encode([1, Atom.to_string(kind), nonce, issue_identifier, owner_session, restart_id | fields])
    else
      _ -> {:error, :invalid_issue_capability_response}
    end
  end

  defp response_message(_kind, _nonce, _issue_identifier, _owner_session, _restart_id, _payload),
    do: {:error, :invalid_issue_capability_response}

  defp response_fields(:progress, payload) do
    changed = value(payload, :changed)
    progress_fingerprint = value(payload, :progress_fingerprint)
    review_fingerprint = value(payload, :review_fingerprint)

    if is_boolean(changed) and hash?(progress_fingerprint) and hash?(review_fingerprint) do
      {:ok, [changed, progress_fingerprint, review_fingerprint]}
    else
      {:error, :invalid_issue_capability_response}
    end
  end

  defp response_fields(:review, payload) do
    authorized = value(payload, :authorized)
    authorization = value(payload, :authorization)
    kind = value(payload, :kind)
    review_round_count = value(payload, :review_round_count)
    security_review_count = value(payload, :security_review_count)
    review_fingerprint = value(payload, :review_fingerprint)
    requested_head = value(payload, :requested_head)

    if valid_review_response?(
         authorized,
         authorization,
         kind,
         review_round_count,
         security_review_count,
         review_fingerprint,
         requested_head
       ) do
      {:ok,
       [
         authorized,
         authorization,
         kind,
         review_round_count,
         security_review_count,
         review_fingerprint,
         requested_head
       ]}
    else
      {:error, :invalid_issue_capability_response}
    end
  end

  defp valid_review_response?(
         authorized,
         authorization,
         kind,
         review_round_count,
         security_review_count,
         review_fingerprint,
         requested_head
       ) do
    authorized == true and hash?(authorization) and kind in ["full", "delta", "security"] and
      non_negative_integer?(review_round_count) and
      non_negative_integer?(security_review_count) and hash?(review_fingerprint) and
      commit_sha?(requested_head)
  end

  defp non_negative_integer?(value), do: is_integer(value) and value >= 0

  defp canonical_term(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> [to_string(key), canonical_term(nested)] end)
    |> Enum.sort()
  end

  defp canonical_term(value) when is_list(value), do: Enum.map(value, &canonical_term/1)
  defp canonical_term(value), do: value

  defp value(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
  defp hash?(value), do: is_binary(value) and Regex.match?(~r/^[0-9a-f]{64}$/, value)
  defp commit_sha?(value), do: is_binary(value) and Regex.match?(~r/^[0-9a-f]{40}$/, value)

  defp sign(key, message) do
    :crypto.mac(:hmac, :sha256, key, message)
    |> Base.url_encode64(padding: false)
  end

  defp secure_equal?(left, right) when is_binary(left) and is_binary(right) do
    byte_size(left) == byte_size(right) and Plug.Crypto.secure_compare(left, right)
  end

  defp secure_equal?(_left, _right), do: false
end
