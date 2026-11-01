defmodule SymphonyElixir.T3Runner do
  @moduledoc "Runs issues as ordinary, interactive T3 threads. T3 owns their Git worktrees."

  require Logger
  alias SymphonyElixir.{Config, Workflow}
  alias SymphonyElixir.T3.{Client, Store}

  @spec config() :: map()
  def config do
    base = Workflow.workflow_file_path() |> Path.expand() |> Path.dirname()
    settings = Config.settings!().t3

    settings
    |> Map.from_struct()
    |> Map.update!(:state_dir, &Path.expand(&1, base))
    |> Map.update!(:credentials_file, &Path.expand(&1, base))
  end

  @spec start_session(term(), keyword()) :: {:ok, map()} | {:error, term()}
  def start_session(_workspace, opts) do
    config = Keyword.get_lazy(opts, :t3_config, &config/0)
    client = Keyword.get(opts, :t3_client, Client)
    issue = Keyword.fetch!(opts, :issue)

    with {:ok, connection} <- client.connect(config) do
      case client.supports_launch_key?(connection) do
        :ok ->
          {:ok, %{client: client, connection: connection, config: config, path: Store.path(config, issue.id), issue: issue}}

        error ->
          client.close(connection)
          error
      end
    end
  end

  @spec stop_session(map()) :: :ok
  def stop_session(session), do: session.client.close(session.connection)

  @spec run_turn(map(), String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def run_turn(session, prompt, _issue, opts \\ []) do
    on_message = Keyword.get(opts, :on_message, fn _ -> :ok end)

    with {:ok, record} <- :global.trans({{__MODULE__, session.path}, self()}, fn -> run_locked(session, prompt) end) do
      emit_started(on_message, record)
      monitor(session, record, on_message)
    end
  end

  defp run_locked(session, prompt) do
    with {:ok, record} <- Store.load(session.path, session.issue.id),
         {:ok, record} <- ensure_thread(session, record),
         {:ok, record} <- finish_pending_stop(session, record),
         {:ok, record} <- ensure_run(session, record, prompt) do
      {:ok, Store.save!(session.path, Map.put(record, "terminal_stopped", false))}
    end
  end

  @doc "Stops a mapped thread, including queued turns and delegated work. Keeps its checkout and history."
  @spec stop_issue(String.t()) :: :ok | {:error, term()}
  def stop_issue(issue_id), do: stop_issue(issue_id, config(), Client)

  @doc false
  @spec stop_issue(String.t(), map(), module()) :: :ok | {:error, term()}
  def stop_issue(issue_id, config, client) do
    path = Store.path(config, issue_id)

    :global.trans({{__MODULE__, path}, self()}, fn -> stop_locked(path, issue_id, config, client) end)
  end

  defp stop_locked(path, issue_id, config, client) do
    with {:ok, record} <- Store.load(path, issue_id) do
      stop_record(path, issue_id, config, client, record)
    end
  end

  defp stop_record(_path, _issue_id, _config, _client, record) when map_size(record) == 0, do: :ok

  defp stop_record(path, issue_id, config, client, record) do
    record =
      if record["stop_pending"],
        do: record,
        else: Store.save!(path, record |> Map.put("stop_pending", true) |> Map.put("stop_key", key()))

    with {:ok, connection} <- client.connect(config) do
      session = %{path: path, config: config, connection: connection, client: client, issue: %{id: issue_id}}

      try do
        with {:ok, record} <- ensure_thread(session, record), {:ok, _} <- finish_pending_stop(session, record), do: :ok
      after
        client.close(connection)
      end
    end
  end

  @doc "Retries terminal-issue stops, including runs left alive during a Symphony restart."
  @spec reconcile_terminal() :: :ok
  def reconcile_terminal do
    if Config.settings!().agent.backend == "t3" do
      case Store.tracked_issue_ids(config()) do
        {:ok, ids} -> ids |> Enum.chunk_every(100) |> Enum.each(&reconcile_issue_ids/1)
        {:error, reason} -> Logger.warning("T3 terminal reconciliation failed: #{inspect(reason)}")
      end
    end

    :ok
  end

  defp reconcile_issue_ids(ids) do
    case SymphonyElixir.Tracker.fetch_issues_by_ids(ids) do
      {:ok, issues} -> Enum.each(issues, &stop_terminal_issue/1)
      {:error, reason} -> Logger.warning("T3 issue reconciliation failed: #{inspect(reason)}")
    end
  end

  defp stop_terminal_issue(issue) do
    terminal = Config.settings!().tracker.terminal_states |> Enum.map(&String.downcase/1)

    if String.downcase(issue.state) in terminal do
      case stop_issue(issue.id) do
        :ok -> mark_terminal_stopped(issue.id)
        {:error, reason} -> Logger.warning("T3 stop pending issue_id=#{issue.id} issue_identifier=#{issue.identifier}: #{inspect(reason)}")
      end
    end
  end

  defp mark_terminal_stopped(id) do
    path = Store.path(config(), id)

    :global.trans({{__MODULE__, path}, self()}, fn ->
      with {:ok, record} <- Store.load(path) do
        Store.save!(path, Map.put(record, "terminal_stopped", true))
      end
    end)
  end

  defp ensure_thread(session, %{"thread_id" => id} = record) when is_binary(id) do
    deadline = System.monotonic_time(:millisecond) + session.config.request_timeout_ms
    await_workspace(session, record, deadline)
  end

  defp ensure_thread(session, record) do
    record =
      if record == %{},
        do:
          Store.save!(session.path, %{
            "version" => 1,
            "issue_id" => session.issue.id,
            "launch_key" => key(),
            "turn" => 0,
            "credential_id" => session.connection.credential_id
          }),
        else: record

    # Persist the intent before launch. The same key must survive a lost response and restart.
    with :ok <- same_credential(session, record),
         {:ok, launched} <-
           call(session, "t3_thread_launch", %{
             projectId: session.config.project_id,
             title: "Symphony #{Map.get(session.issue, :identifier, session.issue.id)}",
             clientRequestId: record["launch_key"],
             runtimeMode: "approval-required",
             interactionMode: "default",
             workspaceStrategy: %{type: "worktree", baseRef: session.config.base_ref, startFromOrigin: false}
           }),
         id when is_binary(id) <- launched["threadId"] do
      record = Store.save!(session.path, Map.put(record, "thread_id", id))
      ensure_thread(session, record)
    else
      nil -> {:error, :invalid_t3_launch_result}
      error -> error
    end
  end

  defp await_workspace(session, record, deadline) do
    with {:ok, detail} <- call(session, "t3_thread_read", %{threadId: record["thread_id"], limit: 1}) do
      thread = detail["thread"]

      cond do
        thread["projectId"] != session.config.project_id ->
          {:error, :t3_thread_project_mismatch}

        is_binary(thread["worktreePath"]) and thread["worktreePath"] != "" ->
          {:ok, Store.save!(session.path, Map.put(record, "workspace_ready", true))}

        record["workspace_ready"] ->
          {:error, :t3_thread_workspace_mismatch}

        System.monotonic_time(:millisecond) >= deadline ->
          {:error, :t3_workspace_preparing}

        true ->
          Process.sleep(session.config.poll_interval_ms)
          await_workspace(session, record, deadline)
      end
    end
  end

  defp finish_pending_stop(session, %{"stop_pending" => true} = record) do
    with {:ok, _} <-
           call(session, "t3_thread_interrupt", %{
             threadId: record["thread_id"],
             stop: true,
             reason: "Symphony stopped this issue",
             clientRequestId: record["stop_key"]
           }),
         {:ok, detail} <- call(session, "t3_thread_read", %{threadId: record["thread_id"], limit: 1}),
         {:ok, settled} <-
           call(session, "t3_thread_wait", %{
             threadId: record["thread_id"],
             timeoutMs: 1,
             untilDelegatedWorkSettled: true
           }),
         true <- is_nil(detail["thread"]["activeRunId"]) and settled["timedOut"] == false do
      {:ok,
       Store.save!(
         session.path,
         record |> Map.put("stop_pending", false) |> Map.delete("run_id") |> Map.delete("send_key") |> Map.delete("prompt")
       )}
    else
      false -> {:error, :t3_stop_pending}
      error -> error
    end
  end

  defp finish_pending_stop(_session, record), do: {:ok, record}

  defp ensure_run(_session, %{"run_id" => id} = record, _prompt) when is_binary(id), do: {:ok, record}

  defp ensure_run(session, record, prompt) do
    with {:ok, detail} <- call(session, "t3_thread_read", %{threadId: record["thread_id"], limit: 1}) do
      cond do
        record["send_key"] ->
          send_prompt(session, record)

        detail["thread"]["activeRunId"] ->
          {:ok, Store.save!(session.path, Map.put(record, "run_id", detail["thread"]["activeRunId"]))}

        true ->
          record =
            Store.save!(
              session.path,
              record |> Map.put("send_key", key()) |> Map.put("prompt", safe_prompt(prompt)) |> Map.put("credential_id", session.connection.credential_id) |> Map.update!("turn", &(&1 + 1))
            )

          send_prompt(session, record)
      end
    end
  end

  defp send_prompt(session, record) do
    with :ok <- same_credential(session, record),
         {:ok, sent} <-
           call(session, "t3_thread_send", %{
             threadId: record["thread_id"],
             message: record["prompt"],
             mode: "queue",
             clientRequestId: record["send_key"]
           }),
         id when is_binary(id) <- sent["runId"] do
      {:ok, Store.save!(session.path, record |> Map.put("run_id", id) |> Map.delete("prompt"))}
    else
      nil -> {:error, :invalid_t3_send_result}
      error -> error
    end
  end

  defp monitor(session, record, on_message) do
    with {:ok, detail} <-
           call(session, "t3_thread_read", %{
             threadId: record["thread_id"],
             view: "activity",
             limit: 10,
             runLimit: 50
           }) do
      thread = detail["thread"]
      run = Enum.find(detail["recentRuns"], &(&1["runId"] == record["run_id"]))
      status = if run, do: run["status"], else: nil
      emit_status(on_message, record, thread, status)
      monitor_result(session, record, on_message, thread, status)
    end
  end

  defp emit_started(on_message, record) do
    on_message.(%{
      event: :session_started,
      timestamp: DateTime.utc_now(),
      session_id: "#{record["thread_id"]}:#{record["run_id"]}",
      payload: %{backend: "t3", thread_id: record["thread_id"], status: "starting"}
    })
  end

  defp emit_status(on_message, record, thread, status) do
    on_message.(%{
      event: status_event(thread["pendingRequestCount"], status),
      timestamp: DateTime.utc_now(),
      session_id: "#{record["thread_id"]}:#{record["run_id"]}",
      payload: %{
        backend: "t3",
        thread_id: record["thread_id"],
        thread_url: thread["link"],
        workspace_path: thread["worktreePath"],
        status: thread["status"],
        pending_requests: thread["pendingRequestCount"],
        run_status: status
      }
    })
  end

  defp status_event(pending, _status) when pending > 0, do: :approval_required
  defp status_event(_pending, status) when status in ["cancelled", "interrupted", "rolled_back"], do: :t3_cancelled
  defp status_event(_pending, _status), do: :t3_status

  defp monitor_result(session, record, on_message, thread, status) do
    latest = thread["latestRunId"] || thread["activeRunId"]

    cond do
      latest && latest != record["run_id"] ->
        record = Store.save!(session.path, Map.put(record, "run_id", latest))
        emit_started(on_message, record)
        wait(session, record, on_message)

      thread["pendingRequestCount"] > 0 or not is_nil(thread["activeRunId"]) ->
        wait(session, record, on_message)

      status in ["completed", "failed"] ->
        settle_run(session, record, on_message, status)

      is_nil(status) ->
        {:error, {:t3_run_not_found, record["run_id"]}}

      # A human Stop is not a request for Symphony to immediately start the agent again.
      true ->
        wait(session, record, on_message)
    end
  end

  defp settle_run(session, record, on_message, status) do
    with {:ok, result} <-
           call(session, "t3_thread_wait", %{
             threadId: record["thread_id"],
             runId: record["run_id"],
             timeoutMs: 1,
             untilDelegatedWorkSettled: true
           }) do
      if result["timedOut"] do
        wait(session, record, on_message)
      else
        finish_run(session, record, get_in(result, ["resultRun", "status"]) || status)
      end
    end
  end

  defp finish_run(session, record, status) do
    Store.save!(session.path, record |> Map.delete("run_id") |> Map.delete("send_key"))

    if status == "completed" do
      {:ok, %{session_id: "#{record["thread_id"]}:#{record["run_id"]}"}}
    else
      {:error, {:t3_run_failed, record["run_id"]}}
    end
  end

  defp wait(session, record, on_message) do
    receive do
    after
      session.config.poll_interval_ms -> monitor(session, record, on_message)
    end
  end

  defp same_credential(session, record) do
    if record["credential_id"] == session.connection.credential_id do
      :ok
    else
      {:error, :t3_unresolved_intent_from_previous_oauth_session}
    end
  end

  defp call(session, name, args), do: session.client.call(session.connection, name, args)
  defp key, do: "symphony-" <> Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)

  defp safe_prompt(prompt) do
    """
    Symphony safety policy. This overrides any conflicting workflow instructions:
    Work only in this thread's T3-managed worktree. Never merge a branch or pull request,
    enable auto-merge, or bypass an approval. Leave integration and merging to a human.
    Ask the human in T3 Code when an approval or decision is needed.

    #{prompt}
    """
  end
end
