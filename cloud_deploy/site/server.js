const express = require('express');
const path = require('path');

const app = express();
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// Middleware to parse form data
app.use(express.urlencoded({ extended: true }));

// In-memory data storage
let activeUsers = [];

// Define some valid serials for testing
const validSerials = ['ABC123', 'XYZ999', 'TESTSERIAL'];

// Landing Page (GET)
app.get('/', (req, res) => {
  // Remove expired users before rendering
  const now = Date.now();
  activeUsers = activeUsers.filter(user => user.expiresAt > now);

  res.render('index', { activeUsers });
});

// Route to handle form submissions (POST)
app.post('/register', (req, res) => {
  const { username, dca_length, amount, exit_addr, serial } = req.body;

  // Basic validation of fields
  if (!username || !dca_length || !amount || !exit_addr || !serial) {
    return res.send("All fields are required. Please go back and fill out all fields.");
  }

  // Check if serial is valid
  if (!validSerials.includes(serial)) {
    return res.send("Invalid serial. Please provide a valid serial number.");
  }

  const minutes = parseInt(dca_length, 10);
  if (isNaN(minutes) || minutes <= 0) {
    return res.send("DCA length must be a positive number of minutes.");
  }

  // Calculate expiration time
  const expiresAt = Date.now() + (minutes * 60 * 1000);

  // Add user to the active users list
  activeUsers.push({
    username,
    amount,
    exit_addr,
    expiresAt
  });

  // Redirect back to the home page to display the active users
  res.redirect('/');
});

// Start the server
const PORT = 80;  // or choose another port if you like
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
