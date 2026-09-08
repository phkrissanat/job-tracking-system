// ⚠️ ต้องแทนที่ค่าด้านล่างด้วยค่าจริงจากโปรเจกต์ Firebase ใหม่ของคุณ
// วิธีสร้าง: https://console.firebase.google.com → Add project → ตั้งชื่อ เช่น
// "mold-tracking-system" → เปิดใช้ Firestore Database (production mode) และ
// Authentication → Email/Password → จากนั้นไปที่ Project settings → General →
// "Your apps" → Web app (</>) → คัดลอกค่า firebaseConfig มาวางแทนที่นี่
export const firebaseConfig = {
  apiKey: "YOUR_API_KEY",
  authDomain: "YOUR_PROJECT_ID.firebaseapp.com",
  projectId: "YOUR_PROJECT_ID",
  storageBucket: "YOUR_PROJECT_ID.firebasestorage.app",
  messagingSenderId: "YOUR_SENDER_ID",
  appId: "YOUR_APP_ID"
};
