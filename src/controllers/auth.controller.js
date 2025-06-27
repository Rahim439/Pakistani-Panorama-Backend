import { asyncHandler } from "../utils/asyncHandler.js";
import { ApiError } from "../utils/ApiError.js";
import { ApiResponse } from "../utils/ApiResponse.js";
import { User } from "../models/user.model.js";
import crypto from "crypto";

const options = {
  secure: process.env.NODE_ENV === "production",
  httpOnly: true,
  sameSite: 'lax',
  path: '/',
  maxAge: 86400000 // 24 hours in milliseconds
};

const generateAccessAndRefreshToken = async (userId) => {
  try {
    const user = await User.findById(userId);
    const accessToken = await user.generateAccessToken();
    
    // Generate the actual refresh token
    const refreshToken = await user.generateRefreshToken();
    
    // Hash the refresh token before storing
    const hashedRefreshToken = user.createHashedRefreshToken(refreshToken);
    
    // Save the hashed version to the database
    user.refreshToken = hashedRefreshToken;
    await user.save({ validateBeforeSave: false });
    
    // Return the original tokens (not hashed) to the client
    return { accessToken, refreshToken };
  } catch (error) {
    throw new ApiError(500, "Token generation failed");
  }
};

// Google OAuth callback handler
const googleCallback = asyncHandler(async (req, res) => {
  try {
    // Google profile information comes from the passport middleware
    const profile = req.user;
    
    if (!profile || !profile.email) {
      throw new ApiError(400, "Google authentication failed: Missing email");
    }

    
    // Check if user already exists by email
    let user = await User.findOne({ email: profile.email });
    
    if (!user) {
      // Create a new user if they don't exist
      const randomPassword = crypto.randomBytes(20).toString('hex');
      
      let email = profile.email || 'user@example.com';
      let userName = email
                     .split('@')[0]            // take part before "@"
                     .replace(/\s+/g, '')      // strip any spaces
            + Math.floor(Math.random() * 10000);  // add 4-digit suffix

// Ensure username is at least 5 characters long
if (userName.length < 5) {
  userName = userName + 'user' + Math.floor(Math.random() * 1000);
}
      
      user = await User.create({
        userName,
        email: profile.email,
        password: randomPassword, // Random password since they'll use Google to login
        googleId: profile.googleId,
        picture: profile.picture,
        authMethod: "google"
      });
    } else if (!user.googleId) {
      // Update existing user with Google information if they haven't used Google login before
      user.googleId = profile.googleId;
      user.picture = profile.picture || user.picture;
      user.authMethod = "google";
      await user.save({ validateBeforeSave: false });
    }
    
    // Generate tokens
    const { accessToken, refreshToken } = await generateAccessAndRefreshToken(user._id);
    
    // Determine redirect URL (frontend URL where user should be redirected after successful login)
    const redirectUrl = `${process.env.FRONTEND_URL || 'http://localhost:5173'}/auth/google/success?token=${accessToken}`;
    
    // Set cookies and redirect
    res.cookie("accessToken", accessToken, options);
    res.cookie("refreshToken", refreshToken, options);
    res.redirect(redirectUrl);
    
  } catch (error) {
    console.error("Google auth error:", error);
    // Redirect to error page on frontend
    res.redirect(`${process.env.FRONTEND_URL || 'http://localhost:5173'}/auth/google/error`);
  }
});

export { googleCallback }; 