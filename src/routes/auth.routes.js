import { Router } from "express";
import passport from "passport";
import { googleCallback } from "../controllers/auth.controller.js";

const router = Router();

// Route to initiate Google OAuth
router.get(
  "/google",
  passport.authenticate("google", { scope: ["profile", "email"] })
);

// Google OAuth callback route
router.get(
  "/google/callback",
  passport.authenticate("google", { 
    session: false,
    failureRedirect: `${process.env.FRONTEND_URL || 'http://localhost:5173'}/auth/google/error`
  }),
  googleCallback
);

export default router; 