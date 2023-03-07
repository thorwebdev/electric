// Safe entrypoint for tests that avoids importing the React Native
// specific dependencies.
import { DbName } from '../../util/types'

import { ElectrifyOptions, electrify } from '../../electric/index'

import { MockMigrator } from '../../migrators/mock'
import { Notifier } from '../../notifiers/index'
import { MockNotifier } from '../../notifiers/mock'
import { MockRegistry } from '../../satellite/mock'

import { DatabaseAdapter } from './adapter'
import { Database } from './database'
import { MockDatabase, MockWebSQLDatabase } from './mock'
import { MockSocketFactory } from '../../sockets/mock'
import { MockConsoleClient } from '../../auth/mock'
import { ElectricConfig } from '../../config'
import { DalNamespace, DbSchemas } from '../../client/model/dalNamespace'

type RetVal<
  S extends DbSchemas,
  N extends Notifier,
  D extends Database = Database
> = Promise<[D, N, DalNamespace<S>]>
const testConfig = { app: 'app', env: 'default', migrations: [] }

export async function initTestable<
  S extends DbSchemas,
  N extends Notifier = MockNotifier
>(name: DbName, dbSchemas: S): RetVal<S, N, MockDatabase>
export async function initTestable<
  S extends DbSchemas,
  N extends Notifier = MockNotifier
>(
  name: DbName,
  dbSchemas: S,
  webSql: false,
  config?: ElectricConfig,
  opts?: ElectrifyOptions
): RetVal<S, N, MockDatabase>
export async function initTestable<
  S extends DbSchemas,
  N extends Notifier = MockNotifier
>(
  name: DbName,
  dbSchemas: S,
  webSql: true,
  config?: ElectricConfig,
  opts?: ElectrifyOptions
): RetVal<S, N, MockWebSQLDatabase>

export async function initTestable<
  S extends DbSchemas,
  N extends Notifier = MockNotifier
>(
  dbName: DbName,
  dbSchemas: S,
  useWebSQLDatabase = false,
  config: ElectricConfig = testConfig,
  opts?: ElectrifyOptions
): RetVal<S, N> {
  const db = useWebSQLDatabase
    ? new MockWebSQLDatabase(dbName)
    : new MockDatabase(dbName)

  const adapter = opts?.adapter || new DatabaseAdapter(db)
  const migrator = opts?.migrator || new MockMigrator()
  const notifier = (opts?.notifier as N) || new MockNotifier(dbName)
  const socketFactory = opts?.socketFactory || new MockSocketFactory()
  const console = opts?.console || new MockConsoleClient()
  const registry = opts?.registry || new MockRegistry()

  const dal = await electrify(
    dbName,
    dbSchemas,
    adapter,
    migrator,
    notifier,
    socketFactory,
    console,
    registry,
    config
  )
  return [db, notifier, dal]
}
