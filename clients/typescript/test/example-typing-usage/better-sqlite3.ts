import Database from 'better-sqlite3'

import { electrify } from '../../src/drivers/better-sqlite3'
import { dbDescription } from '../client/generated'

const config = {
  app: 'app',
  env: 'env',
  migrations: [],
}

const original = new Database('example.db')

// Electrify the DB and use the DAL to query the `items` table
const { db } = await electrify(original, dbDescription, config)
await db.items.findMany({
  select: {
    value: true,
  },
})
