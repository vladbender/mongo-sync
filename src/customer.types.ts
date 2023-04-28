import type { ObjectId } from 'mongodb';

export type Customer = {
  firstName: string;
  lastName: string;
  email: string;
  address: {
    line1: string;
    line2: string;
    postcode: string;
    city: string;
    state: string;
    country: string;
  };
};

export type CustomerBeforeInsert = Customer & {
  createdAt: Date;
};

export type CustomerDocument = CustomerBeforeInsert & {
  _id: ObjectId;
};
