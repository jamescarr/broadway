defmodule OffBroadway.Jetstream.AcknowledgerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias OffBroadway.Jetstream.Acknowledger

  describe "configure/3" do
    test "merges valid actions into the ack data" do
      assert Acknowledger.configure(:ack_ref, %{reply_to: "r"}, on_failure: {:nack, 10}) ==
               {:ok, %{reply_to: "r", on_failure: {:nack, 10}}}
    end

    test "rejects unknown options and invalid actions" do
      assert_raise ArgumentError, "unsupported configure_ack option :unknown", fn ->
        Acknowledger.configure(:ack_ref, %{reply_to: "r"}, unknown: :ack)
      end

      assert_raise ArgumentError, ~r/expected one of .* got: :bogus/, fn ->
        Acknowledger.configure(:ack_ref, %{reply_to: "r"}, on_success: :bogus)
      end
    end
  end

  test "ack/3 stops at the first unreachable connection, logs once and returns :ok" do
    ack_ref = {Acknowledger, :"ack_test_#{System.unique_integer([:positive])}"}

    Acknowledger.put_config(ack_ref, %{
      connection_name: :not_a_gnat_connection,
      on_success: :ack,
      on_failure: :nack
    })

    on_exit(fn -> :persistent_term.erase(ack_ref) end)

    message = fn reply_to, ack_data ->
      %Broadway.Message{
        data: "data",
        acknowledger: {Acknowledger, ack_ref, Map.put(ack_data, :reply_to, reply_to)}
      }
    end

    successful = [message.("$JS.ACK.S.C.1.1.1.0.0", %{}), message.("r2", %{on_success: :noop})]
    failed = [message.("r3", %{}), message.("r4", %{})]

    log =
      capture_log(fn ->
        assert Acknowledger.ack(ack_ref, successful, failed) == :ok
      end)

    # The :noop message needs no call. The first of the other three exits, so all three
    # are left for JetStream to redeliver, with a single warning.
    assert [_] = Regex.scan(~r/could not reach Gnat connection/, log)
    assert log =~ "skipped 3 acknowledgement(s)"
  end
end
