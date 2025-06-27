import { asyncHandler } from "../utils/asyncHandler.js";
import { ApiError } from "../utils/ApiError.js";
import { ApiResponse } from "../utils/ApiResponse.js";
import fetch from "node-fetch";  // You'll need to install this

const callGeminiAPI = asyncHandler(async (req, res) => {
  const { message } = req.body;
  
  if (!message) {
    throw new ApiError(400, "Message is required");
  }
  
  const API_KEY = process.env.GEMINI_API_KEY;
  const API_URL = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${API_KEY}`;

  try {
    const response = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        contents: [{ parts: [{ text: message }] }]
      })
    });

    const data = await response.json();
    
    console.log("Gemini API Response:", JSON.stringify(data, null, 2));
    
    if (!data?.candidates?.[0]?.content?.parts?.[0]?.text) {
      if (data?.error) {
        throw new ApiError(500, `Gemini API Error: ${data.error.message || JSON.stringify(data.error)}`);
      }
      throw new ApiError(500, "Failed to get response from Gemini");
    }
    
    const botResponse = data.candidates[0].content.parts[0].text;

    return res.status(200).json(
      new ApiResponse(
        200,
        { response: botResponse },
        "AI response generated successfully"
      )
    );
  } catch (error) {
    console.error("Error calling Gemini API:", error);
    throw new ApiError(500, error.message || "Failed to generate AI response");
  }
});

export { callGeminiAPI };