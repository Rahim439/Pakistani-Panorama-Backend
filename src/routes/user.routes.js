import { Router } from "express";
import {
  registerUser,
  loginUser,
  logoutUser,
  refreshAccessToken,
  changeCurrentPassword,
  getCurrentUser,
  updatePassword,
  updateAccountDetails,
  forgotPassword,
  resetPassword
} from "../controllers/user.controller.js";

import { verifyJWT } from "../middlewares/auth.middleware.js"
export const router = Router();
router.route("/register").post(registerUser);

router.route("/login").post(loginUser);
router.route("/update-profile").put(verifyJWT, updateAccountDetails);
//secured routes (user must be logged in)
router.route("/logout").post(logoutUser);
router.route("/refresh-token").post(refreshAccessToken);
// Password reset routes (no authentication required)
router.route("/forgot-password").post(forgotPassword);
router.route("/reset-password/:token").post(resetPassword);
// Add the current user endpoint
router.route("/current-user").get(verifyJWT, getCurrentUser);

//update password
router.route("/update-password").put(verifyJWT, updatePassword);
export default router;
