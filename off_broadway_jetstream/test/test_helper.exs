case :gen_tcp.connect(~c"localhost", 4222, [:binary]) do
  {:ok, socket} ->
    :gen_tcp.close(socket)

  {:error, reason} ->
    Mix.raise(
      "Cannot connect to nats-server at localhost:4222 (#{:inet.format_error(reason)}). " <>
        "Start one with: docker run --rm -d -p 4222:4222 nats:2.15.2-alpine -js"
    )
end

{:ok, _} =
  Gnat.ConnectionSupervisor.start_link(%{
    name: :gnat,
    connection_settings: [%{host: "localhost", port: 4222}]
  })

Enum.find(1..50, fn _ ->
  Process.whereis(:gnat) || (Process.sleep(100) && false)
end) || Mix.raise("Gnat connection :gnat did not start within 5 seconds")

ExUnit.start(capture_log: true, assert_receive_timeout: 2_000)
