import cookieParser from "cookie-parser";
import express from "express";
import cors from "cors";
import session from "express-session";
import setupPassport from "./config/passport.config.js";
import { errorHandler } from "./middlewares/error.middleware.js";
import { connectRedis } from "./config/redis.config.js";

const app = express();

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
    },
  })
);

// Initialize passport
const passport = setupPassport();
app.use(passport.initialize());

app.use(
  cors({
    credentials: true,
    origin: process.env.FRONTEND_URL || "http://localhost:5173",
  })
);

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
