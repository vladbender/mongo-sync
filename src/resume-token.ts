import path from 'node:path';
import fs from 'node:fs/promises';
import type { ResumeToken } from 'mongodb';

const resumeTokenPath = path.join(__dirname, '../resume_token.json');

export const loadResumeToken = async (): Promise<ResumeToken | null> => {
  const resumeToken = await fs
    .readFile(resumeTokenPath, 'utf-8')
    .then((content) => JSON.parse(content))
    .catch((err) => {
      if (err.code === 'ENOENT') {
        return null;
      }
      throw err;
    });
  return resumeToken;
};

export const saveResumeToken = async (resumeToken: unknown) => {
  return fs
    .writeFile(resumeTokenPath, JSON.stringify(resumeToken))
    .catch((err) => {
      console.error('Error while writing resume token file', err);
    });
};
