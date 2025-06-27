import { asyncHandler } from "../utils/asyncHandler.js";
import { ApiError } from "../utils/ApiError.js";
import { ApiResponse } from "../utils/ApiResponse.js";

import { User } from "../models/user.model.js";
import jwt from "jsonwebtoken";

import crypto from "crypto";
import nodemailer from "nodemailer";

const options = {
  secure: false,
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

const registerUser = asyncHandler(async (req, res) => {
  // Get user details from frontend
  const { userName, email, password,confirmPassword } = req.body;
  console.log("User Details : ", userName, email, password);

  // Validation (not empty)
  if (!userName || !email || !password || !confirmPassword) {
    throw new ApiError(400, "Please provide all details");
  }
  if (password !== confirmPassword) {
    throw new ApiError(400, "Passwords do not match");
  }
  // Check if email is valid
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    throw new ApiError(400, "Please provide a valid email");
  }

  // Check if user already exists
  const existedUser = await User.findOne({userName});
  if (existedUser) {
    throw new ApiError(409, "User already exists");
  }

 

  // Create user
  const newUser = await User.create({
    userName,
    email,
    password,
  });

  // Remove password and refresh token from response
  const createdUser = await User.findById(newUser._id).select(
    "-password -refreshToken"
  );

  // Check if user is created
  if (!createdUser) {
    throw new ApiError(500, "User not created");
  }

  // Send response
  return res
    .status(201)
    .json(new ApiResponse(201, createdUser, "User Registered successfully"));
});

const loginUser = asyncHandler(async (req, res) => {
  const { email,password } = req.body;
  console.log(email, password);

  if (!email && !password) {
    throw new ApiError(400, "Email or password is required");
  }

  const user = await User.findOne({email});

  if (!user) {
    throw new ApiError(404, "User does not exist");
  }

  //check password
  const isPasswordValid = await user.isPasswordCorrect(password);
  if (!isPasswordValid) {
    throw new ApiError(401, "Password does not match");
  }

  const { accessToken, refreshToken } = await generateAccessAndRefreshToken(
    user._id
  );
  const loggedInUser = await User.findById(user._id).select(
    "-password -refreshToken"
  );

  return res
    .status(200)
    .cookie("accessToken", accessToken, options)
    .cookie("refreshToken", refreshToken, options)
    .json(
      new ApiResponse(
        200,
        { user: loggedInUser,
          token: accessToken
        },
        "User logged in successfully"
      )
    );
});

//updated logout function implementation
const logoutUser = asyncHandler(async (req, res) => {
  try {
    // Try to get user ID from cookie if possible
    let userId = null;
    
    const token = req.cookies?.accessToken;
    if (token) {
      try {
        // Attempt to decode the token without verifying
        const decodedToken = jwt.decode(token);
        userId = decodedToken?._id;
      } catch (error) {
        // Ignore token decoding errors
        console.log("Failed to decode token during logout");
      }
    }
    
    // If we have a userId, clean up the database
    if (userId) {
      await User.findByIdAndUpdate(
        userId,
        { $set: { refreshToken: null } },
        { new: true }
      );
    }
  } catch (error) {
    // Don't fail the logout if database cleanup fails
    console.error("Database cleanup during logout failed:", error);
  }
  
  // Always clear cookies, regardless of database cleanup
  return res
    .status(200)
    .clearCookie("accessToken", options)
    .clearCookie("refreshToken", options)
    .json(new ApiResponse(200, {}, "User logged out"));
});

const refreshAccessToken = asyncHandler(async (req, res) => {
  const incomingRefreshToken = req.cookies.refreshToken || req.body.refreshToken;
  
  if (!incomingRefreshToken) {
    throw new ApiError(401, "Refresh token not provided");
  }
  
  try {
    // Verify token signature
    const decodedToken = jwt.verify(incomingRefreshToken, process.env.REFRESH_TOKEN_SECRET);
    
    // Find the user
    const user = await User.findById(decodedToken?._id);
    if (!user) {
      throw new ApiError(404, "User not found");
    }
    
    // Hash the incoming token to compare with stored hash
    const hashedIncomingToken = user.createHashedRefreshToken(incomingRefreshToken);
    
    // Compare the hashed versions
    if (hashedIncomingToken !== user.refreshToken) {
      throw new ApiError(401, "Invalid refresh token");
    }
    
    // Generate new tokens
    const { accessToken, refreshToken: newRefreshToken } = await generateAccessAndRefreshToken(user._id);
    
    // Send back response with new tokens
    return res
      .status(200)
      .cookie("accessToken", accessToken, options)
      .cookie("refreshToken", newRefreshToken, options)
      .json(
        new ApiResponse(
          200,
          { accessToken, refreshToken: newRefreshToken },
          "Access token refreshed"
        )
      );
  } catch (error) {
    throw new ApiError(401, error?.message || "Invalid refresh token");
  }
});
const changeCurrentPassword = asyncHandler(async (req, res) => {
  const { currentPassword, newPassword } = req.body;
  const user = await User.findById(req.user._id);
  if (!user) {
    throw new ApiError(404, "User not found");
  }
  const isPasswordCorrect = await user.isPasswordCorrect(currentPassword);
  if (!isPasswordCorrect) {
    throw new ApiError(401, "Current password is incorrect");
  }
  user.password = newPassword;
  await user.save({ validateBeforeSave: false });
  return res
  .status(200)
  .json(new ApiResponse(200, {}, "Password changed successfully"));
});


const getCurrentUser = asyncHandler(async (req, res) => {
  return res
    .status(200)
    .json(new ApiResponse(200, req.user, "Current user fetched successfully"));
}); 

const updateAccountDetails = asyncHandler(async (req, res) => {
  const { userName, email } = req.body
  if (!userName && !email) {
    throw new ApiError(400, "Please provide at least one field to update");
  }
  
  const user = await User.findByIdAndUpdate(req.user._id, { 
    $set: {
      userName,
      email,
    },
   }, { new: true }).select("-password -refreshToken");
   
  return res
    .status(200)
    .json(new ApiResponse(200, user, "User updated successfully"));
});




/**
 * Configures and returns an email transporter for sending emails
 */
const createTransporter = () => {
  const transporter = nodemailer.createTransport({
    service: process.env.EMAIL_SERVICE, // 'gmail'
    auth: {
      user: process.env.EMAIL_USERNAME,
      pass: process.env.EMAIL_PASSWORD
    },
    // Add these settings
    tls: {
      rejectUnauthorized: false // Helps with some development environments
    }
  });
  
  return transporter;
};

/**
 * Sends an email with the provided options
 */
const sendEmail = async (options) => {
  const transporter = createTransporter();
  
  const mailOptions = {
    from: {
      name: process.env.APP_NAME || "Your App",
      address: process.env.EMAIL_FROM
    },
    to: options.email,
    subject: options.subject,
    html: options.html,
    // Add these headers to improve deliverability
    headers: {
      'Priority': 'High',
      'X-Priority': '1',
      'Importance': 'high'
    }
  };
  
  return await transporter.sendMail(mailOptions);
};

/**
 * Handles forgot password request and sends password reset email
 * @route POST /api/v1/users/forgot-password
 */
const forgotPassword = asyncHandler(async (req, res) => {
  // 1. Get user's email from request body
  const { email } = req.body;
  
  if (!email) {
    throw new ApiError(400, "Email is required");
  }
  
  // 2. Check if user exists with this email
  const user = await User.findOne({ email: email.toLowerCase() });
  
  if (!user) {
    throw new ApiError(404, "No user found with this email");
  }
  
  // 3. Generate random reset token
  const resetToken = crypto.randomBytes(32).toString("hex");
  
  // 4. Hash the token for security before storing in DB
  const hashedToken = crypto
    .createHash('sha256')
    .update(resetToken)
    .digest('hex');
  
  // 5. Set token and expiry (10 minutes from now)
  user.resetPasswordToken = hashedToken;
  user.resetPasswordExpiry = new Date(Date.now() + 10 * 60 * 1000); // 10 minutes
  
  // 6. Save to database without validation
  await user.save({ validateBeforeSave: false });
  
  // 7. Create reset URL
  const resetUrl = `${process.env.FRONTEND_URL}/reset-password/${resetToken}`;
  
  // 8. Create email content
  const html = `
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #e0e0e0; border-radius: 5px;">
      <h2 style="color: #333;">Password Reset Request</h2>
      <p>Hello ${user.userName},</p>
      <p>You recently requested to reset your password for your account. Use the button below to reset it:</p>
      <div style="text-align: center; margin: 30px 0;">
        <a href="${resetUrl}" style="background-color:rgb(66, 244, 93); color: white; padding: 12px 24px; text-decoration: none; border-radius: 4px; font-weight: bold;">Reset Password</a>
      </div>
      <p>This password reset link is only valid for the next 10 minutes.</p>
      <p>If you did not request a password reset, please ignore this email or contact support if you have questions.</p>
      <p>Thanks,<br>The ${process.env.APP_NAME} Team</p>
      <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
      <p style="font-size: 12px; color: #777;">If you're having trouble with the button, copy and paste this URL into your browser:</p>
      <p style="font-size: 12px; color: #777; word-break: break-all;">${resetUrl}</p>
    </div>
  `;
  
  // 9. Send email
  try {
    await sendEmail({
      email: user.email,
      subject: "Password Reset Request",
      html
    });
    
    // 10. Send success response (don't include token in response)
    return res
      .status(200)
      .json(
        new ApiResponse(
          200, 
          {}, 
          "Password reset instructions sent to your email"
        )
      );
      
  } catch (error) {
    // 11. If email fails, clean up token and throw error
    user.resetPasswordToken = undefined;
    user.resetPasswordExpiry = undefined;
    await user.save({ validateBeforeSave: false });
    
    console.error("Email error:", error);
    throw new ApiError(500, "Could not send password reset email. Please try again later.");
  }
});

/**
 * Resets user password using valid token
 * @route POST /api/v1/users/reset-password/:token
 */
const resetPassword = asyncHandler(async (req, res) => {
  // 1. Get token from URL params and password from body
  const { token } = req.params;
  const { password, confirmPassword } = req.body;
  
  // 2. Basic validation
  if (!password || !confirmPassword) {
    throw new ApiError(400, "Password and confirm password are required");
  }
  
  if (password !== confirmPassword) {
    throw new ApiError(400, "Passwords do not match");
  }
  
  // 3. Hash the token to compare with stored hash
  const hashedToken = crypto
    .createHash('sha256')
    .update(token)
    .digest('hex');
  
  // 4. Find user with this token and valid expiry
  const user = await User.findOne({
    resetPasswordToken: hashedToken,
    resetPasswordExpiry: { $gt: Date.now() }
  });
  
  if (!user) {
    throw new ApiError(400, "Token is invalid or has expired");
  }
  
  // 5. Set new password (will be hashed by pre-save hook)
  user.password = password;
  
  // 6. Clear reset token fields
  user.resetPasswordToken = undefined;
  user.resetPasswordExpiry = undefined;
  
await user.save();
  
  // 8. Optional: Clear refresh tokens to force re-login
  user.refreshToken = undefined;
  await user.save({ validateBeforeSave: false });
  
  // 9. Return success response
  return res
    .status(200)
    .json(
      new ApiResponse(
        200, 
        {}, 
        "Password has been reset successfully. Please log in with your new password."
      )
    );
});

//update password
const updatePassword = asyncHandler(async (req, res) => {
  const { currentPassword, newPassword, confirmPassword } = req.body;
  const user = await User.findById(req.user._id);
  if (!user) {
    throw new ApiError(404, "User not found");
  }
  if (newPassword !== confirmPassword) {
    throw new ApiError(400, "Passwords do not match");
  }
  const isPasswordCorrect = await user.isPasswordCorrect(currentPassword);
  if (!isPasswordCorrect) {
    throw new ApiError(401, "Current password is incorrect");
  }
  user.password = newPassword;
  await user.save({ validateBeforeSave: false });
  
  return res
    .status(200)
    .json(new ApiResponse(200, {}, "Password updated successfully"));
});


export { registerUser, loginUser, logoutUser, refreshAccessToken,updatePassword, changeCurrentPassword, getCurrentUser, updateAccountDetails, forgotPassword, resetPassword};
