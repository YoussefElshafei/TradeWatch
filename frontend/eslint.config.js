// ESLint flat configuration.
//
// Type-aware linting is on for TypeScript sources: the rules that catch real bugs in async
// React code — floating promises, `any` leaking through a component — need type
// information, and a linter that cannot see types mostly reformats code.
//
// The type-aware configs are scoped to `**/*.{ts,tsx}` so that this file, which is plain
// JavaScript and outside the TypeScript project, is still linted but not type-checked.
//
// Formatting is Prettier's job. `eslint-config-prettier` goes last so it switches off every
// stylistic rule the two would otherwise argue about.

import js from '@eslint/js';
import { defineConfig, globalIgnores } from 'eslint/config';
import prettier from 'eslint-config-prettier';
import reactHooks from 'eslint-plugin-react-hooks';
import reactRefresh from 'eslint-plugin-react-refresh';
import globals from 'globals';
import tseslint from 'typescript-eslint';

export default defineConfig([
  globalIgnores(['dist/**', 'coverage/**', 'node_modules/**']),

  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      js.configs.recommended,
      tseslint.configs.strictTypeChecked,
      tseslint.configs.stylisticTypeChecked,
      // `configs.flat.*` is the flat-config namespace; the top-level `configs` export is
      // still the eslintrc shape, which ESLint 10 rejects.
      reactHooks.configs.flat['recommended-latest'],
    ],
    languageOptions: {
      ecmaVersion: 2023,
      globals: globals.browser,
      parserOptions: {
        projectService: true,
        tsconfigRootDir: import.meta.dirname,
      },
    },
    rules: {
      // An unused variable is either a mistake or a leftover; an underscore prefix is how
      // to say "deliberately ignored".
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_' },
      ],
      // Money and timestamps cross the wire as strings (ADR-0009). Interpolating a number
      // or a boolean is fine; interpolating an object is how "[object Object]" reaches a
      // user.
      '@typescript-eslint/restrict-template-expressions': [
        'error',
        { allowNumber: true, allowBoolean: true },
      ],
    },
  },

  {
    files: ['src/**/*.tsx'],
    ...reactRefresh.configs.vite,
  },

  {
    files: ['**/*.{test,spec}.{ts,tsx}', 'tests/**/*.ts'],
    rules: {
      // Test doubles legitimately produce values the type system cannot narrow.
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-unsafe-member-access': 'off',
    },
  },

  {
    files: ['**/*.js'],
    extends: [js.configs.recommended],
    languageOptions: { globals: globals.node },
  },

  prettier,
]);
