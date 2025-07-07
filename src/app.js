import cookieParser from "cookie-parser";
import express from "express";
import cors from "cors";
import session from "express-session";
import setupPassport from "./config/passport.config.js";
import { errorHandler } from "./middlewares/error.middleware.js";
import { connectRedis } from "./config/redis.config.js";

const app = express();

// Fixed: Handle multiple origins properly
const allowedOrigins = [
  "https://www.pakistanipanorama.live", // With www
  "https://pakistanipanorama.live", // Without www
  "https://pakistani-panorama.vercel.app", // Vercel deployment
  "http://localhost:5173", // Local development
  "http://localhost:3000", // Alternative local port
];

// If you have environment variable, split and add those too
if (process.env.FRONTEND_URL) {
  const envOrigins = process.env.FRONTEND_URL.split(",");
  allowedOrigins.push(...envOrigins);
}

console.log("Allowed CORS origins:", allowedOrigins);

// Connect to Redis Cloud
connectRedis();

// Configure session for passport
app.use(
  session({
    secret: process.env.SESSION_SECRET || "your_session_secret",
    resave: false,
    saveUninitialized: false,
    cookie: {
      secure: process.env.NODE_ENV === "production",
      maxAge: 24 * 60 * 60 * 1000, // 24 hours
      sameSite: process.env.NODE_ENV === "production" ? "none" : "lax", // Important for cross-origin
    },
  })
);

// Initialize passport
const passport = setupPassport();
app.use(passport.initialize());

// Fixed CORS configuration to handle multiple origins
app.use(
  cors({
    origin: function (origin, callback) {
      // Allow requests with no origin (like mobile apps or curl requests)
      if (!origin) return callback(null, true);

      console.log(`[CORS] Checking origin: ${origin}`);

      if (allowedOrigins.includes(origin)) {
        console.log(`[CORS] Origin allowed: ${origin}`);
        callback(null, true);
      } else {
        console.log(`[CORS] Origin blocked: ${origin}`);
        console.log(`[CORS] Allowed origins: ${allowedOrigins.join(", ")}`);
        callback(new Error(`Not allowed by CORS. Origin: ${origin}`));
      }
    },
    credentials: true,
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allowedHeaders: [
      "Content-Type",
      "Authorization",
      "X-Requested-With",
      "Accept",
      "Origin",
    ],
    exposedHeaders: ["set-cookie"],
    optionsSuccessStatus: 200, // For legacy browser support
  })
);

// Handle preflight requests for all routes
app.options("*", cors());

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static("public"));
app.use(cookieParser());

//import routes
import chatRouter from "./routes/chat.routes.js";
import userRouter from "./routes/user.routes.js";
import authRouter from "./routes/auth.routes.js";
import emergencyRouter from "./routes/emergency.routes.js";
import virtualTourRouter from "./routes/virtualTour.routes.js";
import natureRouter from "./routes/nature.routes.js";
import attractionsRouter from "./routes/attractions.routes.js";

//route declarations
app.use("/api/v1/chat", chatRouter);
app.use("/api/v1/users", userRouter);
app.use("/api/v1/auth", authRouter);
app.use("/api/v1/emergency", emergencyRouter);
app.use("/api/v1/virtual-tour", virtualTourRouter);
app.use("/api/v1/nature", natureRouter);
app.use("/api/v1/attractions", attractionsRouter);

// Error handler middleware (should be after all route declarations)
app.use(errorHandler);

export { app };
