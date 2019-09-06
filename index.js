#!/usr/bin/env node

/*
 * High-level overview of the migration process:
 *
 *    list all app files
 *    read file content and upload file to S3 (concurrency: 10 files)
 *
 */

const AWS = require('aws-sdk')
const Promise = require('bluebird')
const mongodb = require('mongodb')
const MongoClient = mongodb.MongoClient
const GridFSBucket = mongodb.GridFSBucket
const mime = require('mime')
const yargs = require('yargs')

const args = yargs
  .version('0.0.1')
  .option('mongoUri', {
    alias: 'm',
    required: true,
    describe: 'Mongo URI (mongodb://...)'
  })
  .option('dbName', {
    alias: 'd',
    describe: 'Database name (default: the mongoUri db)'
  })
  .option('folder', {
    alias: 'f',
    required: true,
    describe: 'folder'
  })
  .option('bucket', {
    alias: 'b',
    describe: 'bucket name',
    required: true
  })
  .option('region', {
    alias: 'r',
    describe: 'bucket region',
    default: 'us-east-1'
  })
  .option('accessKeyId', {
    alias: 'k',
    describe: 'AWS access key id (default: system config)'
  })
  .option('secretAccessKey', {
    alias: 's',
    describe: 'AWS secret access key (default: system config)'
  })
  .option('concurrency', {
    alias: 'c',
    describe: 'Number of uploads at same time',
    default: 10,
    type: 'number'
  })
  .implies('accessKeyId', 'secretAccessKey')
  .help()
  .alias('help', 'h')
  .alias('version', 'v')
  .argv

const {mongoUri, dbName, appId: folder, bucket, region, concurrency, accessKeyId, secretAccessKey} = args

console.log('Your options: ', {mongoUri, dbName, folder, bucket, region, concurrency, accessKeyId, secretAccessKey})


const dbOptions = {useNewUrlParser: true, useUnifiedTopology: true}

// setup AWS credentials
if (secretAccessKey && accessKeyId) {
  AWS.config.update({accessKeyId, secretAccessKey})
}

// setup AWS clients
const s3 = new AWS.S3({
  params: {Bucket: bucket},
  region: region
})

let db
let connection
let gridFs

connectToMongoDB()
  .then(migrateAppFiles)
  .then(closeConnections)
  .catch(function (err) {
    logError(err)
    return closeConnections()
  })

function connectToMongoDB () {
  console.log('Opening connection to MongoDB')
  return MongoClient.connect(mongoUri, dbOptions)
    .then(function (_connection) {
      connection = _connection
      db = dbName ? connection.db(dbName) : connection.db()
    })
}

function migrateAppFiles () {
  return listAppFiles().then(function (files) {
    console.log('Copying', files.length, 'files')
    // migrate 10 files at a time
    return Promise.map(files, migrateFile, {concurrency})
  })
}

function listAppFiles () {
  gridFs = new GridFSBucket(db)
  const cursor = gridFs.find()
  return cursor.toArray()
}

function migrateFile (file) {
  console.log('getting file from mongo', file.filename)
  const stream = readFileFromMongo(file)
  return writeFileToS3(file, stream)
}

function readFileFromMongo (file) {
  return gridFs.openDownloadStreamByName(file.filename)
}

function writeFileToS3 (file, data) {
  const params = {
    ACL: 'public-read',
    Key: folder ? `${folder}/${file.filename}`: file.filename,
    Body: data,
    ContentType: mime.getType(file.filename)
  }
  return s3.upload(params).promise().then(() => {
    console.log(`copied to s3 https://${bucket}/${params.Key}`)
  })
}

function closeConnections () {
  console.log('Closing connections')
  return connection && connection.close()
}

function logError (err) {
  console.error(err)
  yargs.showHelp()
}
