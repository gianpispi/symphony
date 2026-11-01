defmodule SymphonyElixir.T3.Store do
  @moduledoc "Per-issue recovery records, kept separately from T3-owned worktrees."
  # shortcut: one Symphony VM owns a state directory, add an OS lease before supporting multiple instances.

  @spec path(map(), String.t()) :: Path.t()
  def path(config, issue_id) do
    key = :crypto.hash(:sha256, "#{config.endpoint}\n#{config.project_id}\n#{issue_id}") |> Base.encode16(case: :lower)
    Path.join([config.state_dir, "issues", key <> ".json"])
  end

  @spec tracked_issue_ids(map()) :: {:ok, [String.t()]} | {:error, term()}
  def tracked_issue_ids(config) do
    config.state_dir
    |> Path.join("issues/*.json")
    |> Path.wildcard()
    |> Enum.reduce_while({:ok, []}, fn path, {:ok, ids} ->
      case load(path) do
        {:ok, %{"terminal_stopped" => true}} -> {:cont, {:ok, ids}}
        {:ok, %{"issue_id" => id}} -> {:cont, {:ok, [id | ids]}}
        _ -> {:halt, {:error, :invalid_t3_recovery_record}}
      end
    end)
  end

  @spec load(Path.t(), String.t() | nil) :: {:ok, map()} | {:error, term()}
  def load(path, expected_id \\ nil) do
    case File.read(path) do
      {:ok, text} -> decode(text, expected_id)
      {:error, :enoent} -> {:ok, %{}}
      {:error, reason} -> {:error, {:t3_state_read, reason}}
    end
  end

  defp decode(text, expected_id) do
    with {:ok, %{} = record} <- Jason.decode(text),
         true <- valid_required?(record),
         true <- is_nil(expected_id) or record["issue_id"] == expected_id,
         true <- valid_references?(record) and valid_intents?(record) do
      {:ok, record}
    else
      _ -> {:error, :invalid_t3_recovery_record}
    end
  end

  defp valid_required?(%{"version" => 1, "issue_id" => id, "launch_key" => key, "credential_id" => credential, "turn" => turn}) do
    Enum.all?([id, key, credential], &nonempty_string?/1) and is_integer(turn) and turn >= 0
  end

  defp valid_required?(_), do: false

  defp valid_references?(record) do
    refs_valid =
      Enum.all?(["thread_id", "run_id", "send_key", "stop_key"], fn key ->
        not Map.has_key?(record, key) or nonempty_string?(record[key])
      end)

    flags_valid =
      Enum.all?(["stop_pending", "workspace_ready", "terminal_stopped"], fn key ->
        not Map.has_key?(record, key) or is_boolean(record[key])
      end)

    refs_valid and flags_valid
  end

  defp valid_intents?(record) do
    (record["stop_pending"] != true or Map.has_key?(record, "stop_key")) and
      (not Map.has_key?(record, "run_id") or Map.has_key?(record, "thread_id")) and
      (not Map.has_key?(record, "send_key") or Map.has_key?(record, "run_id") or is_binary(record["prompt"]))
  end

  defp nonempty_string?(value), do: is_binary(value) and byte_size(value) > 0

  @spec save!(Path.t(), map()) :: map()
  def save!(path, record) do
    File.mkdir_p!(Path.dirname(path))
    File.chmod!(Path.dirname(path), 0o700)
    temp = path <> ".#{System.unique_integer([:positive])}.tmp"

    try do
      File.open!(temp, [:write, :exclusive, :binary], fn file ->
        File.chmod!(temp, 0o600)
        :ok = IO.binwrite(file, Jason.encode!(record))
        :ok = :file.sync(file)
      end)

      File.rename!(temp, path)
      record
    after
      File.rm(temp)
    end
  end
end
