'use strict'

const admin = require('firebase-admin')
const serviceAccount = require('./tokenServerFirebase')

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: 'https://cupo-escolar.firebaseio.com'
})

const firebaseDB = admin.database()

module.exports = firebaseDB
