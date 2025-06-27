import fetch from "node-fetch";
import { ApiError } from "../utils/ApiError.js";
import { ApiResponse } from "../utils/ApiResponse.js";
import { getRedisClient } from "../config/redis.config.js";

/**
 * Get nearby places based on location and type using Google Places API (New)
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getNearbyPlaces = async (req, res) => {
  try {
    const startTime = Date.now(); // For timing the response
    const { latitude, longitude, type, radius = 10000 } = req.body;
    
    console.log(`[REQUEST] getNearbyPlaces - type: ${type}, location: ${latitude},${longitude}, radius: ${radius}m`);

    if (!latitude || !longitude || !type) {
      throw new ApiError(400, "Latitude, longitude, and type are required");
    }

    // Generate a cache key based on request parameters
    const lat = parseFloat(latitude).toFixed(3);
    const lng = parseFloat(longitude).toFixed(3);
    const cacheKey = `places:${lat}:${lng}:${type}:${radius}`;
    
    console.log(`[CACHE] Looking for key: ${cacheKey}`);
    
    // Try to get data from Redis Cloud cache
    let cachedData;
    let dataSource = "API";
    
    try {
      console.time('redis-connect');
      const client = await getRedisClient();
      console.timeEnd('redis-connect');

      console.time('redis-get');
      cachedData = await client.get(cacheKey);
      console.timeEnd('redis-get');

      console.time('redis-parse');
      const parsedData = JSON.parse(cachedData);
      console.timeEnd('redis-parse');
      
      if (cachedData) {
        dataSource = "CACHE";
        console.log(`[CACHE HIT] Found data in Redis for key: ${cacheKey}`);
        
        console.log(`[CACHE] Retrieved ${parsedData.length} places from cache`);
        
        const responseTime = Date.now() - startTime;
        console.log(`[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Cache)`);
        
        return res.status(200).json(
          new ApiResponse(200, parsedData, "Places fetched from cache")
        );
      }
    } catch (redisError) {
      console.error(`[REDIS ERROR] Failed to get data from cache: ${redisError.message}`);
      // Continue with API call if Redis fails
    }

    console.log(`[CACHE MISS] No data in Redis for key: ${cacheKey}. Fetching from Google Places API...`);

    // Google Places API requires an API key
    const apiKey = process.env.GOOGLE_PLACES_API_KEY;
    
    if (!apiKey) {
      throw new ApiError(500, "Google Places API key is not configured");
    }

    // Using the new Places API format
    const url = "https://places.googleapis.com/v1/places:searchNearby";
    
    const requestBody = {
      includedTypes: [type],
      locationRestriction: {
        circle: {
          center: {
            latitude: parseFloat(latitude),
            longitude: parseFloat(longitude)
          },
          radius: parseInt(radius)
        }
      },
      rankPreference: "DISTANCE",
      maxResultCount: 20
    };

    console.log(`[API REQUEST] Calling Google Places API with type: ${type}`);

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": apiKey,
        "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.location,places.rating,places.userRatingCount,places.currentOpeningHours,places.primaryTypeDisplayName,places.id,places.photos,places.internationalPhoneNumber"
      },
      body: JSON.stringify(requestBody)
    });

    const data = await response.json();

    if (!response.ok) {
      console.error(`[API ERROR] Google Places API returned error: ${JSON.stringify(data.error || {})}`);
      throw new ApiError(response.status, "Error fetching places from Google API", [data.error || "Unknown error"]);
    }

    console.log(`[API SUCCESS] Found ${data.places?.length || 0} places from Google Places API`);
    
    // Store in Redis Cloud cache with 24-hour expiration (86400 seconds)
    if (data.places && data.places.length > 0) {
      try {
        const client = await getRedisClient();
        // When storing data in Redis
        const essentialPlaceData = data.places.map(place => ({
          id: place.id,
          displayName: place.displayName,
          formattedAddress: place.formattedAddress,
          location: place.location,
          currentOpeningHours: place.currentOpeningHours,
          internationalPhoneNumber: place.internationalPhoneNumber,
          photos: place.photos,
          rating: place.rating,
          userRatingCount: place.userRatingCount,
          primaryTypeDisplayName: place.primaryTypeDisplayName
        }));

        await client.set(cacheKey, JSON.stringify(essentialPlaceData), { EX: 86400 });
        
        // Verify that TTL is set correctly
        const ttl = await client.ttl(cacheKey);
        console.log(`[CACHE STORE] Successfully cached ${data.places.length} places with key: ${cacheKey} (TTL: ${ttl} seconds)`);
      } catch (redisError) {
        console.error(`[REDIS ERROR] Failed to cache data: ${redisError.message}`);
        // Continue even if caching fails
      }
    } else {
      console.log(`[CACHE SKIP] No places found, skipping cache storage for key: ${cacheKey}`);
    }
    
    const responseTime = Date.now() - startTime;
    console.log(`[PERFORMANCE] Request completed in ${responseTime}ms (Source: Google Places API)`);
    
    return res.status(200).json(
      new ApiResponse(200, data.places || [], "Places fetched from Google API")
    );
  } catch (error) {
    console.error(`[ERROR] getNearbyPlaces failed: ${error.message}`);
    if (error instanceof ApiError) {
      return res.status(error.statusCode).json(error);
    }
    return res.status(500).json(
      new ApiError(500, "Internal server error", [error.message])
    );
  }
};