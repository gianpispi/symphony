defmodule Mix.Tasks.T3.Demo do
  @shortdoc "Run two harmless turns in one native T3 thread, then stop it"
  @moduledoc "Usage: mix t3.demo path/to/DEMO_WORKFLOW.md. Does not start the scheduler or contact Linear."
  use Mix.Task
  alias SymphonyElixir.{AgentRunner, Config, T3Runner, Workflow}
  alias SymphonyElixir.T3.{Client, Store}
  alias SymphonyElixir.Tracker.Issue

  @impl true
  def run([workflow]) do
    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:req)
    Workflow.set_workflow_file_path(Path.expand(workflow))
    :ok = Config.validate!()

    unless Config.settings!().agent.backend == "t3" and Config.settings!().tracker.kind == "memory" do
      Mix.raise("The demo requires agent.backend: t3 and tracker.kind: memory")
    end

    issue = %Issue{
      id: "symphony-t3-demo",
      identifier: "DEMO-1",
      title: "T3 thread lifecycle demo",
      description: "Reply SYMPHONY_T3_DEMO_OK. Do not call tools, edit files or merge anything.",
      state: "In Progress"
    }

    for _ <- 1..2 do
      :ok = AgentRunner.run(issue, nil, max_turns: 1, issue_state_fetcher: fn _ -> {:ok, []} end)
    end

    config = T3Runner.config()
    {:ok, record} = Store.load(Store.path(config, issue.id))
    {:ok, connection} = Client.connect(config)

    try do
      {:ok, detail} = Client.call(connection, "t3_thread_read", %{threadId: record["thread_id"], limit: 5})
      Mix.shell().info("Two turns used the same thread: #{detail["thread"]["link"]}")
      Mix.shell().info("T3 worktree retained: #{detail["thread"]["worktreePath"]}")
      :ok = T3Runner.stop_issue(issue.id)
      Mix.shell().info("Stopped via T3. No merge or Linear mutation was requested.")
    after
      Client.close(connection)
    end
  end

  def run(_), do: Mix.raise("Usage: mix t3.demo path/to/DEMO_WORKFLOW.md")
end
