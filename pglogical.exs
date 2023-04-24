
defmodule PGLogical do
  def decode(<<"B", _::binary>> = begin) do
    <<"B", flags::unsigned-integer-8, lsn::unsigned-integer-64, commit_time::unsigned-integer-64, xid::unsigned-integer-32>> = begin
  end
end
msg = <<66, 0, 0, 0, 2, 167, 244, 168, 128, 0, 2, 48, 246, 88, 88, 213, 242, 0, 0, 2, 107>>

Electric.Replication.PostgresConnectorMng.start_link("postgres_1")
# Process.sleep(5000)
# Electric.Replication.Postgres.LogicalReplicationProducer.start_link("postgres_1")
# PGLogical.decode(msg)

receive do
  _ -> :ok
end
