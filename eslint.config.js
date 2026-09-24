import parser from '@typescript-eslint/parser';
export default [
  { ignores: ['**/dist/**', '**/node_modules/**'] },
  {
    files: ['**/*.ts', '**/*.tsx'],
    languageOptions: {
      parser,
      parserOptions: { ecmaVersion: 'latest', sourceType: 'module', ecmaFeatures: { jsx: true } },
    },
    rules: { 'no-undef': 'off', 'no-unused-vars': 'off' },
  },
];
