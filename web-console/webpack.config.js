const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const LicensePlugin = require('webpack-license-plugin');
const CopyPlugin = require("copy-webpack-plugin");
const { webpack, DefinePlugin } = require('webpack');
const { GitRevisionPlugin } = require('git-revision-webpack-plugin');
const { readFileSync } = require('fs');

const VERSION = readFileSync('../VERSION').toString().trim();

const COMMIT_HASH = process.env.HASH ?? (new GitRevisionPlugin()).commithash();

var config = {
    entry: './src/index.tsx',
    module: {
        rules: [
            {
                test: /\.tsx?$/,
                use: 'ts-loader',
                exclude: /node_modules/,
            },
            {
                test: /\.css$/i,
                use: ["style-loader", "css-loader"],
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
        }),
        new CopyPlugin({
            patterns: [
                { from: "static", to: "static" },
            ],
        }),
        new DefinePlugin({
            __VERSION__: `"${VERSION}"`,
            __COMMIT_HASH__: `"${COMMIT_HASH}"`,
        }),
        new LicensePlugin({
            outputFilename: 'licenses.json',
            additionalFiles: {
                'licenses.txt': (packages) => {
                    return [
                        "Knitfab Web-Console",
                        "====================",
                        "",
                        "This software uses the following packages:",
                        "",
                        ...packages.map((pkg) => {
                            return [
                                "======",
                                pkg.name,
                                "------ ",
                                'Version: ' + pkg.version,
                                ...(pkg.author ? ['Author: ' + pkg.author] : []),
                                'Repository: ' + pkg.repository,
                                'License: ' + pkg.license,
                                "",
                                pkg.licenseText,
                                "",
                            ].join('\n');
                        }),
                    ].join('\n');
                },
            }
        }),
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

