const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');

var config = {
    entry: './src/index.tsx',
    module: {
        rules: [
            {
                test: /\.tsx?$/,
                use: 'ts-loader',
                exclude: /node_modules/,
            },
        ],
    },
    resolve: {
        extensions: ['.tsx', '.ts', '.js'],
    },
    output: {
        filename: 'index.js',
        path: path.resolve(__dirname, 'dist'),
    },

    plugins: [
        new HtmlWebpackPlugin({
            template: `${__dirname}/index.html`,
        })
    ],
};

try {
    const envjson = require('./env.json');

    config = {
        ...config,
        devtool: 'inline-source-map',
        devServer: {
            static: 'dist',
            compress: true,
            port: 9000,
            proxy: [
                {
                    context: ['/api'],
                    target: envjson.apiRoot,
                    secure: false,
                }
            ],
            client: {
                overlay: false,
            },
        }
    }
} catch (e) {
    console.log('env.json not found');
}

module.exports = config;

