defmodule Electric.Postgres.TestConnection do
  def config do
    user = System.get_env("USER")

    [
      host: System.get_env("PG_HOST", "localhost"),
      port: System.get_env("PG_PORT"),
      database: System.get_env("PG_DB", "electric_pgx"),
      username: System.get_env("PG_USERNAME", user),
      password: System.get_env("PGPASSWORD")
    ]
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
    |> Enum.map(fn {k, v} -> {k, to_charlist(v)} end)
  end

  def childspec(config) do
    %{
      id: :epgsql,
      start: {:epgsql, :connect, [config]}
    }
  end
end
