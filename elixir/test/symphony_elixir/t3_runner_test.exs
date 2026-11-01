defmodule SymphonyElixir.T3TestServer do
  import Plug.Conn

  def init(state), do: state

  def call(conn, state) do
    {:ok, body, conn} = read_body(conn)
    message = if body == "", do: %{}, else: Jason.decode!(body)
    {status, result} = Agent.get_and_update(state, &reply(message, &1))
    envelope = %{jsonrpc: "2.0", id: message["id"], result: result}

    configured_mode = Agent.get(state, &Map.get(&1, :response_mode))
    mode = if is_map(configured_mode), do: Map.get(configured_mode, get_in(message, ["params", "name"])), else: configured_mode
    {content_type, response} = encode_response(mode, envelope)

    conn
    |> put_resp_header("mcp-session-id", "test-session")
    |> put_resp_content_type(content_type)
    |> send_resp(status, response)
  end

  defp encode_response(:json, envelope), do: {"application/json", Jason.encode!(envelope)}
  defp encode_response(:text_json, envelope), do: {"text/plain", Jason.encode!(envelope)}
  defp encode_response(:invalid_type, _), do: {"application/json", "[]"}
  defp encode_response(:bad_sse, _), do: {"text/event-stream", "data: not-json\n\n"}
  defp encode_response(:missing_id, envelope), do: encode_response(:json, %{envelope | id: -1})
  defp encode_response(:rpc_error, envelope), do: encode_response(:json, Map.put(envelope, :error, %{code: -1}))
  defp encode_response(:invalid_envelope, envelope), do: encode_response(:json, Map.delete(envelope, :result))

  defp encode_response(:tool_error, envelope),
    do: encode_response(:json, %{envelope | result: %{isError: true, content: []}})

  defp encode_response(:invalid_tool_result, envelope), do: encode_response(:json, %{envelope | result: %{}})
  defp encode_response(:no_launch_key, envelope), do: encode_response(:json, %{envelope | result: %{tools: []}})
  defp encode_response(_, envelope), do: {"text/event-stream", ": keepalive\n\nevent: message\ndata: #{Jason.encode!(envelope)}\n\n"}

  defp reply(%{"method" => "initialize"}, state), do: {{200, %{protocolVersion: "2025-06-18"}}, state}

  defp reply(%{"method" => "tools/list"}, state) do
    {{200, %{tools: [%{name: "t3_thread_launch", inputSchema: %{properties: %{clientRequestId: %{type: "string"}}}}]}}, state}
  end

  defp reply(%{"method" => "tools/call", "params" => params}, state) do
    case get_in(state, [:tool_results, params["name"]]) do
      nil ->
        {status, content, state} = tool(params["name"], params["arguments"], state)
        {{status, %{structuredContent: content}}, state}

      content ->
        {{200, %{structuredContent: content}}, state}
    end
  end

  defp reply(_, state), do: {{200, %{}}, state}

  defp tool("t3_thread_launch", args, state) do
    key = args["clientRequestId"]
    thread = Map.get(state.launches, key, "thread-#{map_size(state.launches) + 1}")
    state = %{state | launches: Map.put(state.launches, key, thread), launch_args: args}
    {if(state.fail_launch, do: 503, else: 200), %{"threadId" => thread}, %{state | fail_launch: false}}
  end

  defp tool("t3_thread_send", args, state) do
    key = args["clientRequestId"]
    run = Map.get(state.sends, key, "run-#{map_size(state.sends) + 1}")
    state = %{state | sends: Map.put(state.sends, key, run), send_args: args, runs: Map.put_new(state.runs, run, state.next_status)}
    {if(state.fail_send, do: 503, else: 200), %{"runId" => run}, %{state | fail_send: false}}
  end

  defp tool("t3_thread_read", args, state) do
    if state[:observer], do: send(state.observer, {:t3_read, args})

    thread = %{
      "threadId" => args["threadId"],
      "projectId" => "project",
      "worktreePath" => state.workspace,
      "activeRunId" => state.active,
      "latestRunId" => state[:latest],
      "pendingRequestCount" => state.pending,
      "status" => if(state.pending > 0, do: "waiting", else: "running"),
      "link" => "http://t3/thread"
    }

    content = %{
      "thread" => thread,
      "recentRuns" =>
        Enum.map(state.runs, fn {id, status} ->
          %{"runId" => id, "status" => status}
        end),
      "items" => []
    }

    {200, content, state}
  end

  defp tool("t3_thread_wait", _args, state) do
    {200, %{"timedOut" => Map.get(state, :background_busy, false)}, state}
  end

  defp tool("t3_thread_interrupt", args, state) do
    if state[:observer], do: send(state.observer, {:t3_stop, args})
    {200, %{"status" => "interrupt_requested"}, %{state | stopped: args, active: nil, pending: 0}}
  end
end

defmodule SymphonyElixir.T3RunnerTest do
  use SymphonyElixir.TestSupport
  alias Mix.Tasks.T3.Demo
  alias SymphonyElixir.{Config.Schema, T3Runner}
  alias SymphonyElixir.T3.{Client, Store}

  setup do
    root = Path.join(System.tmp_dir!(), "symphony-t3-test-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)

    {:ok, state} =
      start_supervised(
        {Agent,
         fn ->
           %{
             launches: %{},
             sends: %{},
             runs: %{},
             active: nil,
             pending: 0,
             fail_launch: false,
             fail_send: false,
             launch_args: nil,
             send_args: nil,
             next_status: "completed",
             stopped: nil,
             workspace: Path.join(root, "t3-worktree")
           }
         end}
      )

    {:ok, server} = start_supervised({Bandit, plug: {SymphonyElixir.T3TestServer, state}, port: 0})
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    endpoint = "http://127.0.0.1:#{port}/mcp"
    credentials = Path.join(root, "oauth.json")
    File.write!(credentials, Jason.encode!(%{endpoint: endpoint, access_token: "test-token"}))
    config = %{endpoint: endpoint, project_id: "project", state_dir: Path.join(root, "state"), credentials_file: credentials, base_ref: "HEAD", poll_interval_ms: 10, request_timeout_ms: 1_000}
    issue = %{id: "linear-issue-id", identifier: "SYM-1"}
    on_exit(fn -> File.rm_rf!(root) end)
    %{state: state, config: config, issue: issue, root: root}
  end

  defp session(context) do
    {:ok, session} = T3Runner.start_session(nil, issue: context.issue, t3_config: context.config)
    session
  end

  test "MCP handles JSON, SSE and protocol errors without exposing credentials", ctx do
    session = session(ctx)
    connection = session.connection

    for mode <- [:json, :text_json] do
      Agent.update(ctx.state, &Map.put(&1, :response_mode, mode))
      assert {:ok, _} = Client.call(connection, "t3_thread_read", %{threadId: "thread-1"})
    end

    for {mode, expected} <- [
          {:invalid_type, :invalid_t3_response},
          {:bad_sse, :invalid_t3_sse},
          {:missing_id, :missing_t3_rpc_response},
          {:invalid_envelope, :invalid_t3_rpc_response}
        ] do
      Agent.update(ctx.state, &Map.put(&1, :response_mode, mode))
      assert {:error, ^expected} = Client.call(connection, "t3_thread_read", %{threadId: "thread-1"})
    end

    Agent.update(ctx.state, &Map.put(&1, :response_mode, :rpc_error))
    assert {:error, {:t3_rpc, _}} = Client.call(connection, "t3_thread_read", %{})
    Agent.update(ctx.state, &Map.put(&1, :response_mode, :tool_error))
    assert {:error, {:t3_tool, _, []}} = Client.call(connection, "t3_thread_read", %{})
    Agent.update(ctx.state, &Map.put(&1, :response_mode, :invalid_tool_result))
    assert {:error, {:invalid_t3_result, _}} = Client.call(connection, "t3_thread_read", %{})
    Agent.update(ctx.state, &Map.put(&1, :response_mode, :no_launch_key))
    assert {:error, :t3_launch_idempotency_not_supported} = Client.supports_launch_key?(connection)

    assert {:error, :t3_launch_idempotency_not_supported} =
             T3Runner.start_session(nil, issue: ctx.issue, t3_config: ctx.config)

    Agent.update(ctx.state, &Map.delete(&1, :response_mode))
    # Only tool discovery fails, after initialization has succeeded.
    {:ok, server_session} = Client.connect(ctx.config)
    Agent.update(ctx.state, &Map.put(&1, :response_mode, :no_launch_key))
    assert {:error, :t3_launch_idempotency_not_supported} = Client.supports_launch_key?(server_session)
    Client.close(server_session)
    Client.close(connection)
    missing = %{ctx.config | credentials_file: Path.join(ctx.root, "missing")}
    assert {:error, {:t3_connect, :enoent}} = Client.connect(missing)
    File.write!(ctx.config.credentials_file, "{private-token")
    assert {:error, :invalid_t3_credentials} = Client.connect(ctx.config)
    File.write!(ctx.config.credentials_file, "{}")
    assert {:error, :invalid_t3_credentials} = Client.connect(ctx.config)
    File.write!(ctx.config.credentials_file, Jason.encode!(%{endpoint: "wrong", access_token: "private"}))
    assert {:error, :t3_credentials_endpoint_mismatch} = Client.connect(ctx.config)
    unreachable = put_in(connection.config.endpoint, "http://127.0.0.1:1/mcp")
    assert {:error, :t3_transport_failed} = Client.call(unreachable, "t3_thread_read", %{})
  end

  test "store rejects unreadable or corrupt records and skips terminal history", ctx do
    path = Store.path(ctx.config, ctx.issue.id)
    assert {:error, {:t3_state_read, :eisdir}} = Store.load(ctx.root)
    Store.save!(path, %{"version" => 1, "issue_id" => ctx.issue.id, "terminal_stopped" => true, "launch_key" => "key", "credential_id" => "credential", "turn" => 0})
    assert {:ok, []} = Store.tracked_issue_ids(ctx.config)
    {:ok, record} = Store.load(path)

    assert {:error, :invalid_t3_recovery_record} = Store.load(path, "wrong-issue")

    for invalid <- [Map.delete(record, "launch_key"), Map.put(record, "thread_id", nil), Map.put(record, "stop_pending", "true"), Map.put(record, "stop_pending", true)] do
      Store.save!(path, invalid)
      assert {:error, :invalid_t3_recovery_record} = Store.load(path)
    end

    File.write!(path, "broken")
    assert {:error, :invalid_t3_recovery_record} = Store.tracked_issue_ids(ctx.config)
    assert :ok = T3Runner.stop_issue("unknown", ctx.config, Client)
  end

  test "backend defaults and validation" do
    assert {:ok, settings} = Schema.parse(%{})
    assert settings.agent.backend == "codex"
    assert {:error, _} = Schema.parse(%{agent: %{backend: "other"}})
    assert {:error, _} = Schema.parse(%{agent: %{backend: "t3"}})
    assert {:error, _} = Schema.parse(%{agent: %{backend: "t3"}, t3: %{project_id: "p"}, worker: %{ssh_hosts: ["host"]}})
    assert {:error, _} = Schema.parse(%{t3: %{endpoint: "http://example.com/mcp"}})
    assert {:ok, _} = Schema.parse(%{agent: %{backend: "t3"}, t3: %{project_id: "p"}})
  end

  test "same thread survives session restart and continuation turns", ctx do
    first = session(ctx)
    assert {:ok, _} = T3Runner.run_turn(first, "Rendered issue prompt", ctx.issue)
    assert :ok = T3Runner.stop_session(first)
    second = session(ctx)
    assert {:ok, _} = T3Runner.run_turn(second, "Continuation", ctx.issue)
    T3Runner.stop_session(second)
    state = Agent.get(ctx.state, & &1)
    assert map_size(state.launches) == 1
    assert map_size(state.sends) == 2
    assert state.launch_args["workspaceStrategy"] == %{"type" => "worktree", "baseRef" => "HEAD", "startFromOrigin" => false}
    assert state.launch_args["runtimeMode"] == "approval-required"
    assert state.send_args["mode"] == "queue"
    assert state.send_args["message"] =~ "Never merge"
    assert state.send_args["message"] =~ "Continuation"
    assert {:ok, record} = Store.load(first.path)
    assert record["thread_id"] == "thread-1"
    refute Map.has_key?(record, "run_id")
  end

  test "lost launch and send responses replay their persisted keys", ctx do
    Agent.update(ctx.state, &%{&1 | fail_launch: true})
    first = session(ctx)
    assert {:error, {:t3_http, 503}} = T3Runner.run_turn(first, "Original", ctx.issue)
    T3Runner.stop_session(first)
    Agent.update(ctx.state, &%{&1 | fail_send: true})
    second = session(ctx)
    assert {:error, {:t3_http, 503}} = T3Runner.run_turn(second, "Original", ctx.issue)
    T3Runner.stop_session(second)
    third = session(ctx)
    assert {:ok, _} = T3Runner.run_turn(third, "Must not replace pending prompt", ctx.issue)
    state = Agent.get(ctx.state, & &1)
    assert map_size(state.launches) == 1
    assert map_size(state.sends) == 1
    assert state.send_args["message"] =~ "Original"
    T3Runner.stop_session(third)
  end

  test "worktree preparation finishes before the first prompt is sent", ctx do
    workspace = Agent.get(ctx.state, & &1.workspace)
    parent = self()
    Agent.update(ctx.state, &Map.merge(&1, %{workspace: nil, observer: parent}))
    session = session(ctx)
    task = Task.async(fn -> T3Runner.run_turn(session, "Task", ctx.issue) end)
    assert_receive {:t3_read, _}, 2_000
    assert Agent.get(ctx.state, & &1.sends) == %{}
    Agent.update(ctx.state, &%{&1 | workspace: workspace})
    assert {:ok, _} = Task.await(task)
    Agent.update(ctx.state, &%{&1 | workspace: nil})
    assert {:error, :t3_thread_workspace_mismatch} = T3Runner.run_turn(session, "Retry", ctx.issue)
    T3Runner.stop_session(session)
  end

  test "unfinished delegated work keeps the slot claimed after root completion", ctx do
    Agent.update(ctx.state, &Map.put(&1, :background_busy, true))
    session = session(ctx)
    parent = self()
    task = Task.async(fn -> T3Runner.run_turn(session, "Task", ctx.issue, on_message: fn update -> send(parent, {:status, update}) end) end)
    assert_receive {:status, %{payload: %{run_status: "completed"}}}, 2_000
    assert Task.yield(task, 20) == nil
    Agent.update(ctx.state, &Map.put(&1, :background_busy, false))
    assert {:ok, _} = Task.await(task)
    T3Runner.stop_session(session)
  end

  test "approval waits stay tracked and resume without another message", ctx do
    Agent.update(ctx.state, &%{&1 | pending: 1, next_status: "waiting"})
    session = session(ctx)
    parent = self()
    task = Task.async(fn -> T3Runner.run_turn(session, "Task", ctx.issue, on_message: fn update -> send(parent, {:status, update}) end) end)
    assert_receive {:status, %{event: :approval_required, payload: %{pending_requests: 1}}}, 2_000
    assert Task.yield(task, 20) == nil
    Agent.update(ctx.state, &%{&1 | pending: 0, runs: %{"run-1" => "completed"}})
    assert {:ok, _} = Task.await(task)
    assert map_size(Agent.get(ctx.state, & &1.sends)) == 1
    T3Runner.stop_session(session)
  end

  test "worker restart adopts the in-flight run instead of resending", ctx do
    Agent.update(ctx.state, &%{&1 | next_status: "running", active: "run-1"})
    session = session(ctx)
    # First launch has no active run until its initial send.
    Agent.update(ctx.state, &%{&1 | active: nil})
    parent = self()
    task = Task.async(fn -> T3Runner.run_turn(session, "Task", ctx.issue, on_message: fn update -> send(parent, {:status, update}) end) end)
    assert_receive {:status, _}, 2_000
    Task.shutdown(task, :brutal_kill)
    Agent.update(ctx.state, &%{&1 | active: "run-1", runs: %{"run-1" => "running"}})
    replacement = session(ctx)
    task = Task.async(fn -> T3Runner.run_turn(replacement, "Retry", ctx.issue, on_message: fn update -> send(parent, {:replacement, update}) end) end)
    assert_receive {:replacement, _}, 2_000
    Agent.update(ctx.state, &%{&1 | active: nil, runs: %{"run-1" => "completed"}})
    assert {:ok, _} = Task.await(task)
    assert map_size(Agent.get(ctx.state, & &1.sends)) == 1
    T3Runner.stop_session(session)
    T3Runner.stop_session(replacement)
  end

  test "human cancellation stays paused; a human follow-up is observed", ctx do
    Agent.update(ctx.state, &%{&1 | next_status: "cancelled"})
    session = session(ctx)
    parent = self()
    task = Task.async(fn -> T3Runner.run_turn(session, "Task", ctx.issue, on_message: fn update -> send(parent, {:status, update}) end) end)
    assert_receive {:status, %{event: :t3_cancelled}}, 2_000
    assert Task.yield(task, 20) == nil
    Agent.update(ctx.state, &%{&1 | active: "human-run", runs: Map.put(&1.runs, "human-run", "running")})
    assert_receive {:status, %{session_id: "thread-1:human-run"}}, 2_000
    Agent.update(ctx.state, &%{&1 | active: nil, runs: Map.put(&1.runs, "human-run", "completed")})
    assert {:ok, _} = Task.await(task)
    assert map_size(Agent.get(ctx.state, & &1.sends)) == 1
    T3Runner.stop_session(session)
  end

  test "failed turns retry in the same thread and terminal stops retain worktrees", ctx do
    Agent.update(ctx.state, &%{&1 | next_status: "failed"})
    session = session(ctx)
    assert {:error, {:t3_run_failed, "run-1"}} = T3Runner.run_turn(session, "Task", ctx.issue)
    Agent.update(ctx.state, &%{&1 | next_status: "completed"})
    assert {:ok, _} = T3Runner.run_turn(session, "Retry", ctx.issue)
    workspace = Agent.get(ctx.state, & &1.workspace)
    File.mkdir_p!(workspace)
    File.write!(Path.join(workspace, "human-work"), "keep")
    assert :ok = T3Runner.stop_issue(ctx.issue.id, ctx.config, SymphonyElixir.T3.Client)
    assert Agent.get(ctx.state, & &1.stopped)["stop"] == true
    assert File.read!(Path.join(workspace, "human-work")) == "keep"
    assert map_size(Agent.get(ctx.state, & &1.launches)) == 1
    T3Runner.stop_session(session)
  end

  test "AgentRunner renders workflow and continues in T3 without Symphony workspace hooks", ctx do
    issue = %Issue{id: ctx.issue.id, identifier: ctx.issue.identifier, title: "Test ticket", state: "In Progress", dispatchable: true}
    workflow = Path.join(ctx.root, "WORKFLOW.md")

    config = %{
      tracker: %{kind: "memory"},
      agent: %{backend: "t3", max_turns: 2},
      t3: ctx.config,
      hooks: %{after_create: "exit 9", before_run: "exit 9", after_run: "exit 9"}
    }

    File.write!(workflow, "---\n#{Jason.encode!(config)}\n---\nImplement {{ issue.identifier }}: {{ issue.title }}")
    Workflow.set_workflow_file_path(workflow)
    assert :ok = Config.validate!()
    assert :ok = AgentRunner.run(issue, self(), issue_state_fetcher: fn _ -> {:ok, [issue]} end)
    assert_receive {:codex_worker_update, _, %{payload: %{thread_id: "thread-1"}}}
    state = Agent.get(ctx.state, & &1)
    assert map_size(state.launches) == 1
    assert map_size(state.sends) == 2
    assert state.send_args["message"] =~ "Continuation guidance"
    terminal_issue = %{issue | state: "Done"}
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [terminal_issue])
    assert :ok = T3Runner.reconcile_terminal()
    assert Agent.get(ctx.state, & &1.stopped)["stop"] == true
    output = ExUnit.CaptureIO.capture_io(fn -> Demo.run([workflow]) end)
    assert output =~ "Two turns used the same thread"
    assert output =~ "Stopped via T3"
  end

  test "demo requires an explicit memory tracker workflow" do
    assert_raise Mix.Error, fn -> Demo.run([]) end
    assert_raise Mix.Error, fn -> Demo.run([Workflow.workflow_file_path()]) end
  end

  test "orchestrator tracks approvals and stops terminal issues without deleting T3 files", ctx do
    :sys.suspend(SymphonyElixir.Orchestrator)
    on_exit(fn -> :sys.resume(SymphonyElixir.Orchestrator) end)
    issue = %Issue{id: ctx.issue.id, identifier: ctx.issue.identifier, title: "Test ticket", state: "In Progress", dispatchable: true}
    workflow = Path.join(ctx.root, "WORKFLOW.md")
    config = %{tracker: %{kind: "memory"}, polling: %{interval_ms: 30_000}, agent: %{backend: "t3", max_turns: 1}, t3: ctx.config}
    File.write!(workflow, "---\n#{Jason.encode!(config)}\n---\nReply OK")
    Workflow.set_workflow_file_path(workflow)
    assert :ok = Config.validate!()
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])
    Agent.update(ctx.state, &%{&1 | pending: 1, next_status: "waiting"})
    parent = self()
    Agent.update(ctx.state, &Map.put(&1, :observer, parent))
    name = Module.concat(__MODULE__, :Orchestrator)
    start_supervised!({SymphonyElixir.AgentRuntimeSupervisor, name: Module.concat(__MODULE__, :Runtime), orchestrator_name: name, task_supervisor_name: Module.concat(__MODULE__, :Tasks)})
    assert_receive {:t3_read, %{"view" => "activity"}}, 2_000
    # Wait for an approval heartbeat, not a timer-based guess about dispatch.
    assert_receive {:t3_read, %{"view" => "activity"}}, 2_000
    snapshot = SymphonyElixir.Orchestrator.snapshot(name, 1_000)
    assert [%{t3_thread_id: "thread-1", last_codex_event: :approval_required}] = snapshot.running
    payload = SymphonyElixirWeb.Presenter.state_payload(name, 1_000)
    assert [%{t3_thread_url: "http://t3/thread", agent_backend: "t3"}] = payload.running
    assert hd(payload.running).last_message =~ "Respond in T3 Code"
    workspace = Agent.get(ctx.state, & &1.workspace)
    File.mkdir_p!(workspace)
    File.write!(Path.join(workspace, "keep"), "human work")
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [%{issue | state: "Done"}])
    send(Process.whereis(name), :tick)
    assert_receive {:t3_stop, _}, 2_000
    assert %{running: []} = SymphonyElixir.Orchestrator.snapshot(name, 1_000)
    assert File.read!(Path.join(workspace, "keep")) == "human work"
  end

  test "stop acknowledgements retain an intent until delegated work actually settles", ctx do
    session = session(ctx)
    assert {:ok, _} = T3Runner.run_turn(session, "Task", ctx.issue)
    Agent.update(ctx.state, &Map.put(&1, :background_busy, true))
    assert {:error, :t3_stop_pending} = T3Runner.stop_issue(ctx.issue.id, ctx.config, Client)
    {:ok, first} = Store.load(session.path)
    assert first["stop_pending"]
    Agent.update(ctx.state, &Map.put(&1, :background_busy, false))
    assert :ok = T3Runner.stop_issue(ctx.issue.id, ctx.config, Client)
    {:ok, second} = Store.load(session.path)
    refute second["stop_pending"]
    assert second["stop_key"] == first["stop_key"]
    T3Runner.stop_session(session)
  end

  test "malformed launch and send results never start untracked work", ctx do
    session = session(ctx)
    Agent.update(ctx.state, &Map.put(&1, :tool_results, %{"t3_thread_launch" => %{}}))
    assert {:error, :invalid_t3_launch_result} = T3Runner.run_turn(session, "Task", ctx.issue)
    Agent.update(ctx.state, &Map.put(&1, :tool_results, %{"t3_thread_send" => %{}}))
    assert {:error, :invalid_t3_send_result} = T3Runner.run_turn(session, "Task", ctx.issue)
    assert Agent.get(ctx.state, & &1.sends) == %{}
    T3Runner.stop_session(session)
  end

  test "a user-started run is adopted before Symphony sends any prompt", ctx do
    Agent.update(ctx.state, &%{&1 | active: "human-run", runs: %{"human-run" => "completed"}})
    session = session(ctx)

    assert {:ok, _} =
             T3Runner.run_turn(session, "Must not be sent", ctx.issue,
               on_message: fn
                 %{event: :session_started} -> Agent.update(ctx.state, &%{&1 | active: nil})
                 _ -> :ok
               end
             )

    assert Agent.get(ctx.state, & &1.sends) == %{}
    T3Runner.stop_session(session)
  end

  test "terminal reconciliation logs failed stops, corrupt state and tracker outages", ctx do
    workflow = Path.join(ctx.root, "WORKFLOW.md")
    config = %{tracker: %{kind: "memory"}, agent: %{backend: "t3"}, t3: ctx.config}
    File.write!(workflow, "---\n#{Jason.encode!(config)}\n---\nReply OK")
    Workflow.set_workflow_file_path(workflow)
    assert :ok = Config.validate!()
    session = session(ctx)
    assert {:ok, _} = T3Runner.run_turn(session, "Task", ctx.issue)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [%Issue{id: ctx.issue.id, identifier: ctx.issue.identifier, state: "Done"}])
    Agent.update(ctx.state, &Map.put(&1, :response_mode, %{"t3_thread_interrupt" => :tool_error}))
    assert capture_log(fn -> T3Runner.reconcile_terminal() end) =~ "T3 stop pending"
    {:ok, record} = Store.load(session.path)
    assert record["stop_pending"]
    File.write!(session.path, "corrupt")
    assert capture_log(fn -> T3Runner.reconcile_terminal() end) =~ "T3 terminal reconciliation failed"
    Store.save!(session.path, record)
    config = %{config | tracker: %{kind: "linear", api_key: "test", project_slug: "project", endpoint: ctx.config.endpoint}}
    File.write!(workflow, "---\n#{Jason.encode!(config)}\n---\nReply OK")
    assert :ok = Config.validate!()
    assert capture_log(fn -> T3Runner.reconcile_terminal() end) =~ "T3 issue reconciliation failed"
    T3Runner.stop_session(session)
  end

  test "corrupt state and changed OAuth identity fail closed", ctx do
    path = Store.path(ctx.config, ctx.issue.id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "broken")
    session = session(ctx)
    assert {:error, :invalid_t3_recovery_record} = T3Runner.run_turn(session, "Task", ctx.issue)
    File.rm!(path)
    Agent.update(ctx.state, &%{&1 | fail_launch: true})
    assert {:error, _} = T3Runner.run_turn(session, "Task", ctx.issue)
    changed = put_in(session.connection.credential_id, "different-credential")
    assert {:error, :t3_unresolved_intent_from_previous_oauth_session} = T3Runner.run_turn(changed, "Retry", ctx.issue)
    assert map_size(Agent.get(ctx.state, & &1.launches)) == 1
    T3Runner.stop_session(session)
  end
end
