import dotenv from 'dotenv';
import { Collection, MongoClient } from 'mongodb';
import { faker } from '@faker-js/faker';
import { CustomerBeforeInsert } from './customer.types';
import { log } from './utils';

const CUSTOMERS_COLLECTION = 'customers';
const GENERATION_INTERVAL = 200;
const GENERATION_BATCH_SIZE = 10;

async function main() {
  dotenv.config();
  const dbUri = process.env.DB_URI;
  if (!dbUri) {
    console.error("DB_URI env doesn't provided");
    process.exit(2);
  }
  const client = new MongoClient(dbUri);

  await client.connect();

  const collection = client.db().collection(CUSTOMERS_COLLECTION);

  // Такой подход не гарантирует нам точные 200 секунд,
  // но позволит не запускать одну генерацию не дождавшись окончания другой
  const repeat = (interval: number) => {
    return setTimeout(() => {
      const start = new Date().getTime();
      createAndInsert(collection)
        .catch((err) => {
          console.error('Error while inserting docs', err);
        })
        .finally(() => {
          const end = new Date().getTime();
          const interval = GENERATION_INTERVAL - (end - start);
          timerId = repeat(interval < 0 ? 0 : interval);
        });
    }, interval);
  };

  let timerId = repeat(GENERATION_INTERVAL);

  process.on('SIGINT', () => {
    clearTimeout(timerId);

    client
      .close()
      .catch((err) => {
        console.error('Error while closing connection');
        console.error(err);
        process.exit(3);
      })
      .then(() => {
        console.log('\nGoodbye!');
        process.exit(0);
      });
  });
}

main().catch((err) => {
  console.error('Unknown error occured', err);
  process.exit(1);
});

async function createAndInsert(collection: Collection) {
  const customers = createFakeCustomers();
  const result = await collection.insertMany(customers);
  log('Inserted:');
  Object.values(result.insertedIds).forEach((id) => {
    log(id.toString());
  });
}

function createFakeCustomers() {
  return Array.from({
    length: faker.datatype.number({ min: 1, max: GENERATION_BATCH_SIZE }),
  }).map(() => createFakeCustomer());
}

// faker даёт возможность генерировать некоторые данные
// приближенные к реальности, например email по имени и фамилии
// или zipCode по штату, в остальном же нет гарантии "реальности"
// данных.
// Т.к. задача этой программы - генерировать данные для проверки
// синхронизации, то "реальность" и не требуется
function createFakeCustomer(): CustomerBeforeInsert {
  const state = faker.address.stateAbbr();
  const firstName = faker.name.firstName();
  const lastName = faker.name.lastName();
  return {
    firstName,
    lastName,
    email: faker.internet.email(firstName, lastName),
    address: {
      line1: faker.address.streetAddress(false),
      line2: faker.address.secondaryAddress(),
      postcode: faker.address.zipCodeByState(state),
      city: faker.address.cityName(),
      state,
      country: faker.address.countryCode(),
    },
    createdAt: new Date(),
  };
}
