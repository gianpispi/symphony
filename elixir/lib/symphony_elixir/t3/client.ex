defmodule SymphonyElixir.T3.Client do
  @moduledoc "HTTP MCP client for an outside T3 agent. Credentials come from the OAuth setup script."

  @spec connect(map()) :: {:ok, map()} | {:error, term()}
  def connect(config) do
    with {:ok, text} <- File.read(config.credentials_file),
         {:ok, %{"access_token" => token, "endpoint" => endpoint}} <- credentials(text),
         true <- endpoint == config.endpoint,
         session = %{config: config, token: token, session_id: nil, credential_id: Base.encode16(:crypto.hash(:sha256, token))},
         {:ok, initialized, session} <-
           request(session, "initialize", %{
             protocolVersion: "2025-06-18",
             capabilities: %{},
             clientInfo: %{name: "symphony", version: "1"}
           }),
         session = Map.put(session, :protocol, initialized["protocolVersion"]),
         {:ok, _, session} <- post(session, %{jsonrpc: "2.0", method: "notifications/initialized"}) do
      {:ok, session}
    else
      false -> {:error, :t3_credentials_endpoint_mismatch}
      {:error, :invalid_t3_credentials} -> {:error, :invalid_t3_credentials}
      {:error, reason} -> {:error, {:t3_connect, reason}}
    end
  end

  defp credentials(text) do
    case Jason.decode(text) do
      {:ok, %{"access_token" => token, "endpoint" => endpoint} = value}
      when is_binary(token) and byte_size(token) > 0 and is_binary(endpoint) ->
        {:ok, value}

      _ ->
        {:error, :invalid_t3_credentials}
    end
  end

  @spec close(map()) :: :ok
  def close(session) do
    Req.delete(session.config.endpoint, headers: headers(session), retry: false, redirect: false, receive_timeout: session.config.request_timeout_ms)
    :ok
  end

  @spec call(map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def call(session, tool, args) do
    with {:ok, result, _} <- request(session, "tools/call", %{name: tool, arguments: args}) do
      case result do
        %{"isError" => true} -> {:error, {:t3_tool, tool, result["content"]}}
        %{"structuredContent" => content} when is_map(content) -> {:ok, content}
        _ -> {:error, {:invalid_t3_result, tool}}
      end
    end
  end

  @spec supports_launch_key?(map()) :: :ok | {:error, term()}
  def supports_launch_key?(session) do
    with {:ok, result, _} <- request(session, "tools/list", %{}) do
      tools = if is_map(result), do: Map.get(result, "tools", []), else: []
      if is_list(tools) and Enum.any?(tools, &launch_key_schema?/1), do: :ok, else: {:error, :t3_launch_idempotency_not_supported}
    end
  end

  defp launch_key_schema?(tool) do
    tool["name"] == "t3_thread_launch" and
      is_map(get_in(tool, ["inputSchema", "properties", "clientRequestId"]))
  end

  defp request(session, method, params) do
    id = System.unique_integer([:positive])

    with {:ok, body, session} <- post(session, %{jsonrpc: "2.0", id: id, method: method, params: params}),
         {:ok, envelopes} <- decode(body),
         %{} = envelope <- Enum.find(envelopes, &(&1["id"] == id)) do
      case envelope do
        %{"error" => error} -> {:error, {:t3_rpc, error}}
        %{"result" => result} -> {:ok, result, session}
        _ -> {:error, :invalid_t3_rpc_response}
      end
    else
      nil -> {:error, :missing_t3_rpc_response}
      error -> error
    end
  end

  defp post(session, message) do
    case Req.post(session.config.endpoint,
           json: message,
           headers: headers(session),
           retry: false,
           redirect: false,
           receive_timeout: session.config.request_timeout_ms,
           connect_options: [timeout: session.config.request_timeout_ms]
         ) do
      {:ok, %{status: status, body: body, headers: response_headers}} when status in 200..299 ->
        session_id = List.first(Map.get(response_headers, "mcp-session-id", [])) || session.session_id
        {:ok, body, %{session | session_id: session_id}}

      {:ok, %{status: status}} ->
        {:error, {:t3_http, status}}

      {:error, _} ->
        {:error, :t3_transport_failed}
    end
  end

  defp headers(session) do
    [
      {"authorization", "Bearer #{session.token}"},
      {"accept", "application/json, text/event-stream"},
      {"mcp-protocol-version", Map.get(session, :protocol) || "2025-06-18"}
    ] ++ if(session.session_id, do: [{"mcp-session-id", session.session_id}], else: [])
  end

  defp decode(body) when is_map(body), do: {:ok, [body]}

  defp decode(body) when is_binary(body) do
    if String.starts_with?(String.trim_leading(body), "{") do
      with {:ok, value} <- Jason.decode(body), do: {:ok, [value]}
    else
      body
      |> String.split(~r/\r?\n\r?\n/, trim: true)
      |> Enum.reduce_while({:ok, []}, &decode_frame/2)
    end
  end

  defp decode(_), do: {:error, :invalid_t3_response}

  defp decode_frame(frame, {:ok, acc}) do
    data = frame |> String.split(~r/\r?\n/) |> Enum.filter(&String.starts_with?(&1, "data:"))
    text = Enum.map_join(data, "\n", &(&1 |> String.trim_leading("data:") |> String.trim_leading()))

    case text do
      "" ->
        {:cont, {:ok, acc}}

      _ ->
        case Jason.decode(text) do
          {:ok, envelope} -> {:cont, {:ok, [envelope | acc]}}
          _ -> {:halt, {:error, :invalid_t3_sse}}
        end
    end
  end
end
