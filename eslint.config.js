const yml = require("eslint-plugin-yml");

module.exports = [
  {
    files: ["*.yml", "*.yaml"],
    plugins: {
      yml,
    },
    languageOptions: {
      parser: require("yaml-eslint-parser"),
    },
    rules: {
      "yml/no-empty-document": "error",
      "yml/no-empty-key": "error",
      "yml/quotes": ["error", { prefer: "double", avoidEscape: true }],
    },
  },
];
