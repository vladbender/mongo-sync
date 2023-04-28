import crypto from 'node:crypto';
import dotenv from 'dotenv';
import {
  ChangeStreamUpdateDocument,
  ChangeStreamInsertDocument,
  ChangeStreamOptions,
  MongoClient,
  ObjectId,
  ResumeToken,
} from 'mongodb';
import { Customer, CustomerDocument } from './customer.types';
import { log } from './utils';
import { loadResumeToken, saveResumeToken } from './resume-token';

const CUSTOMERS_COLLECTION = 'customers';
const ANONYMISED_COLLECTION = 'customers_anonymised';

const MAX_BATCH_SIZE = 1000;
const UPDATES_INTERVAL = 1000;
const REINDEX_BULK_WRITE_SIZE = 100;

async function main() {
  dotenv.config();
  const dbUri = process.env.DB_URI;
  if (!dbUri) {
    console.error("DB_URI env doesn't provided");
    process.exit(2);
  }
  const client = new MongoClient(dbUri);

  await client.connect();

  if (process.argv.includes('--full-reindex')) {
    await fullReindexMode(client);
    await client.close();
  } else {
    const cancel = await syncMode(client);
    process.on('SIGINT', () => {
      cancel()
        .finally(() => client.close())
        .catch((err) => {
          console.error('\nError while closing connection');
          console.error(err);
          process.exit(3);
        })
        .then(() => {
          console.log('\nGoodbye!');
          process.exit(0);
        });
    });
  }
}

main().catch((err) => {
  console.error('Unknown error occured', err);
  process.exit(1);
});

async function fullReindexMode(client: MongoClient) {
  const db = client.db();
  const customersCollection =
    db.collection<CustomerDocument>(CUSTOMERS_COLLECTION);
  const anonymizedCollection = db.collection<CustomerDocument>(
    ANONYMISED_COLLECTION
  );

  const cursor = customersCollection.find();

  let updates = [];
  let total = 0;
  for await (const customer of cursor) {
    const { _id } = customer;
    const anonym = anonymizeCustomer(customer);
    const update = convertCustomerUpdateToDotNotation(anonym);

    updates.push({
      updateOne: {
        filter: { _id },
        update: { $set: update },
        upsert: true,
      },
    });

    if (updates.length >= REINDEX_BULK_WRITE_SIZE) {
      const operations = updates;
      updates = [];
      await anonymizedCollection.bulkWrite(operations);
      total += operations.length;
      log(`Updated ${total} documents`);
    }
  }

  if (updates.length > 0) {
    await anonymizedCollection.bulkWrite(updates);
    total += updates.length;
    log(`Updated ${total} documents`);
  }

  await cursor.close();
}

type Update = {
  _id: ObjectId;
  update: Record<string, unknown>;
  resumeToken: ResumeToken;
};

async function syncMode(client: MongoClient) {
  const db = client.db();
  const customersCollection = db.collection(CUSTOMERS_COLLECTION);
  const anonymizedCollection = db.collection(ANONYMISED_COLLECTION);

  const options: ChangeStreamOptions = {};

  const resumeToken = await loadResumeToken();
  if (resumeToken) {
    log('Continue with resume token');
    options.resumeAfter = resumeToken;
  }

  const changeStream = customersCollection.watch([], options);

  let updates: Array<Update> = [];

  const bulkWriteUpdates = async () => {
    if (!updates.length) {
      return;
    }
    const updatesToWrite = updates;
    const lastResumeToken = updates[updates.length - 1].resumeToken;
    updates = [];

    const operations = updatesToWrite.map(({ _id, update }) => ({
      updateOne: {
        filter: { _id },
        update: { $set: update },
        upsert: true,
      },
    }));

    const length = operations.length;

    await anonymizedCollection
      .bulkWrite(operations)
      .then(() => {
        log(`Successfully wrote ${length} docs`);
      })
      .catch((err) => {
        console.error('Error while bulk write', err);
      });

    await saveResumeToken(lastResumeToken);
  };

  // Такой подход не гарантирует нам точные 1000 секунд,
  // но позволит на запускать один апдейт не дождавшись другого
  const repeat = (interval: number) => {
    return setTimeout(() => {
      const start = new Date().getTime();
      bulkWriteUpdates().finally(() => {
        const end = new Date().getTime();
        const interval = UPDATES_INTERVAL - (end - start);
        timerId = repeat(interval < 0 ? 0 : interval);
      });
    }, interval);
  };

  let timerId = repeat(UPDATES_INTERVAL);

  changeStream.on('change', (change) => {
    if (change.operationType === 'insert') {
      const insertChange =
        change as ChangeStreamInsertDocument<CustomerDocument>;
      const { _id } = insertChange.fullDocument;
      const anonym = anonymizeCustomer(insertChange.fullDocument);
      updates.push({
        _id,
        update: anonym,
        resumeToken: changeStream.resumeToken,
      });
    }

    if (change.operationType === 'update') {
      const updateChange =
        change as ChangeStreamUpdateDocument<CustomerDocument>;
      const { _id } = updateChange.documentKey;
      const { updatedFields } = updateChange.updateDescription;
      /**
       * updateDescription может содержать дополнительно два поля:
       * - removedFields
       *     В ТЗ было указано, что создаются документы определенного формата,
       *     но не было указано, что они могут создаваться частично или из них
       *     могут удаляться данные, поэтому игнорируем это поле
       * - truncatedArrays
       *     Массивов в описанном формате данных нет, поэтому игнорируем это поле
       *
       * Поэтому ориентируемся исключительно на updatedFields
       */

      if (!updatedFields) {
        console.error("Updated fields doesn't provided", updateChange);
        return;
      }

      const update = createUpdate(updatedFields);

      updates.push({
        _id,
        update,
        resumeToken: changeStream.resumeToken,
      });
    }

    if (updates.length >= MAX_BATCH_SIZE) {
      clearTimeout(timerId);
      bulkWriteUpdates().finally(() => {
        timerId = repeat(UPDATES_INTERVAL);
      });
    }
  });

  changeStream.on('error', (err) => {
    console.error('Error while listening changes', err);
  });

  // Возвращаем функцию для закрытия стрима в случае
  // если пользователь захочет прервать выполнение синхронизации
  return async () => {
    clearTimeout(timerId);
    return changeStream.close().catch((err) => {
      console.error('Error while closing change stream', err);
    });
  };
}

function anonymizeCustomer(customer: Partial<Customer>): Partial<Customer> {
  const anonymous: Partial<Customer> = {};

  if (customer.firstName) {
    anonymous.firstName = hash(customer.firstName);
  }

  if (customer.lastName) {
    anonymous.lastName = hash(customer.lastName);
  }

  if (customer.email) {
    // Предполагаем, что почта будет валидной.
    // Если это не так, то нужно поменять логику
    const [emailPart1, emailPart2] = customer.email.split('@');
    anonymous.email = `${hash(emailPart1)}@${emailPart2}`;
  }

  if (customer.address) {
    anonymous.address = customer.address;

    const keys: Array<keyof Customer['address']> = [
      'line1',
      'line2',
      'postcode',
    ];

    for (const key of keys) {
      if (customer.address[key]) {
        anonymous.address[key] = hash(customer.address[key]);
      }
    }
  }

  return anonymous;
}

function hash(input: string) {
  /**
   * Согласно ТЗ нужно заменять строки на 8-значную случайную,
   * но детерменированную последовательность символов `[a-zA-Z\d]`
   *
   * Поэтому нам нужно хешировать строку в нужные символы. Готовых
   * хешей с нужным алфавитом я не нашёл, поэтому можно взять, например,
   * md5. Рассмотреть его как число и отобразить на нужный нам алфавит.
   *
   * Т.к. размер требуемого хеша - всего 8 символов, то можно обрезать эти
   * 8 символов с любой стороны хеша, главное всегда брать одни и те же символы.
   * Это вкупе с md5 даст нам детерменированность.
   *
   * Встают вопросы безопасности и коллизий такого подхода. Но т.к. таких требований
   * в т.з. нет - можно позволить себе такой подход.
   */

  const alphabet =
    'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';

  const md5 = crypto.createHash('md5').update(input).digest('hex');

  let bigint = BigInt('0x' + md5);

  const result = [];

  const length = BigInt(alphabet.length);
  while (bigint > 0n) {
    const index = Number(bigint % length);
    bigint = bigint / length;
    result.push(alphabet[index]);
  }

  return result.join('').substring(0, 8);
}

/**
 * Тайпинги пакета mongodb говорят о том, что поле updatedFields
 * имеет тип Partial<CustomerDocument>, но это не так.
 *
 * Объект может быть как, например
 *   {
 *     firstName: 'One',
 *     lastName: 'Two'
 *     address: { line1: 'Three' },
 *   }
 * Так и с использованием dot notation
 * {
 *   firstName: 'One',
 *   lastName: 'Two',
 *   'address.line1': 'Three'
 * }
 *
 * Поэтому в случае с документами типа Customer
 * можно ограничить тип updatedFields до Record<string, unknown>
 *
 * Дальше код написан с некоторыми допущениями, что в updatedFields
 * приходит валидное обновление документа Customer
 */
function createUpdate(updatedFields: Record<string, unknown>) {
  let rewriteFullAddress = false;

  const customer: Record<string, unknown> = {};
  // 1 этап. В случае наличия объекта с dot notation переделываем
  // его в обычного customer для дальнейшей анонимизации
  for (const key in updatedFields) {
    if (key.startsWith('address.')) {
      const [key1, key2] = key.split('.');
      if (!customer[key1]) {
        customer[key1] = {};
      }
      (customer[key1] as Record<string, unknown>)[key2] = updatedFields[key];
    } else if (key === 'address') {
      // В случае если ключ address представлен как есть
      // - значит есть желание перезаписать весь объект целиком
      rewriteFullAddress = true;
      customer[key] = updatedFields[key];
    } else {
      customer[key] = updatedFields[key];
    }
  }

  // 2 этап. Анонимизация
  const anonym = anonymizeCustomer(customer);

  // 3 этап.
  // В случае если нужно перезаписать весь объект - передаем anonym как есть
  // Если нет - нужно переделать на dot notation, чтобы не перезаписать другие поля
  const update: Record<string, unknown> = rewriteFullAddress
    ? anonym
    : convertCustomerUpdateToDotNotation(anonym);

  return update;
}

function convertCustomerUpdateToDotNotation(customer: Partial<Customer>) {
  const update: Record<string, unknown> = {};
  for (const key in customer) {
    if (key === 'address') {
      for (const nestedKey in customer[key]) {
        update[`${key}.${nestedKey}`] = (
          customer as Record<string, Record<string, unknown>>
        )[key][nestedKey];
      }
    } else {
      update[key] = (customer as Record<string, unknown>)[key];
    }
  }
  return update;
}
