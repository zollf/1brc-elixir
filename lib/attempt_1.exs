path_sample = "./data/weather_stations.csv"
path_1m = "/Users/dylan/git/1brc/data/measurements_1m.txt"
path_10m = "/Users/dylan/git/1brc/data/measurements_10m.txt"
path_100m = "/Users/dylan/git/1brc/data/measurements_100m.txt"
path_1b = "/Users/dylan/git/1brc/data/measurements_1b.txt"

defmodule OneBrc do
  @type stat() :: %{min: float(), max: float(), total: float(), count: integer()}
  @type stats() :: %{binary() => stat()}

  def run(name, path) do
    IO.puts("#{name} processing...")

    {u_secs, %{total: total}} =
      :timer.tc(fn ->
        stats =
          path
          |> File.stream!()
          |> Stream.chunk_every(250_000)
          |> Task.async_stream(fn chunk -> OneBrc.process_chunk(chunk) end, timeout: :infinity)
          |> Enum.map(fn {:ok, res} -> res end)
          |> Enum.reduce(%{}, &merge_stats/2)

        %{
          compiled_stats:
            stats
            |> Enum.sort()
            |> Enum.map(&OneBrc.compile_stat/1),
          total:
            stats
            |> Enum.map(fn {_, %{count: count}} -> count end)
            |> Enum.sum()
        }
      end)

    seconds = Float.round(u_secs / 1000 / 1000, 2)
    IO.puts("#{name} took #{seconds}s to process #{total} records")
  end

  @spec process_chunk(list(binary)) :: stats
  def process_chunk(chunk) do
    chunk
    |> Enum.map(&process_row/1)
    |> Enum.reduce(%{}, &merge_stats/2)
  end

  @spec process_row(binary) :: {binary, float}
  def process_row(stat) when is_binary(stat) do
    stat
    |> String.replace("\n", "")
    |> String.split(";")
    |> then(fn [station, measurement] -> {station, String.to_float(measurement)} end)
    |> then(fn {station, measurement} ->
      %{station => %{min: measurement, max: measurement, total: measurement, count: 1}}
    end)
  end

  @spec merge_stats(stats, stats) :: stats
  def merge_stats(stats1, stats2) do
    stats1
    |> Enum.reduce(stats2, fn {station, stat}, merged_stats ->
      if stat2 = Map.get(merged_stats, station) do
        Map.put(merged_stats, station, add_stats_together(stat, stat2))
      else
        Map.put(merged_stats, station, stat)
      end
    end)
  end

  @spec add_stats_together(stat, stat) :: stat
  def add_stats_together(
        %{min: min1, max: max1, total: t1, count: c1},
        %{min: min2, max: max2, total: t2, count: c2}
      ),
      do: %{min: min(min1, min2), max: max(max1, max2), total: t1 + t2, count: c1 + c2}

  def compile_stat({station, %{min: min, max: max, total: total, count: count}}) do
    "#{station}=#{Float.round(min, 1)}/#{Float.round(total / count, 1)}/#{Float.round(max, 1)}"
  end

  def print(results), do: IO.inspect("{#{Enum.join(results, ", ")}})")
end

OneBrc.run("Sample", path_sample)
OneBrc.run("1m", path_1m)
OneBrc.run("10m", path_10m)
OneBrc.run("100m", path_100m)
OneBrc.run("1b", path_1b)
