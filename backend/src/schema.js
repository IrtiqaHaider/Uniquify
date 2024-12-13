const mongoose = require('mongoose');

// Schema for storing unique IDs
const uniqueIdSchema = new mongoose.Schema({
  id: {
    type: Number,
    required: true,
    unique: true, // Ensures no duplicate IDs
  },
}, { timestamps: true });

const ClientData = mongoose.model('ClientData', uniqueIdSchema, 'ClientData');

module.exports = { ClientData };