export const log = (msg: string) =>
  console.log(new Date().toISOString().split('T')[1], msg);
