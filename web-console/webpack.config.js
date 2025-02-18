const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const LicensePlugin = require('webpack-license-plugin')

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
        new LicensePlugin({
            outputFilename: 'licenses.json',
            excludedPackageTest: (packageName) => {
                if (packageName.startsWith('@types/')) {
                    return true;
                }
                switch (packageName) {
                    // comes from `devDependencies` in "package.json"
                    case 'css-loader':
                    case 'html-webpack-plugin':
                    case 'style-loader':
                    case 'ts-loader':
                    case 'typescript':
                    case 'webpack':
                    case 'webpack-cli':
                    case 'webpack-dev-server':
                    case 'webpack-license-plugin':
                        return true;
                }
                return false;
            },
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

