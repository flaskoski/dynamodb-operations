// webpack.config.js

module.exports = {
  entry: './transactions/main.js',
  target: 'node',
  mode: 'development',
  module: {
    rules: [{
        test: /\.m?[j|t]s$/,
        exclude: /node_modules/,
        use: {
            loader: 'babel-loader'
        }
    }]
  },
};