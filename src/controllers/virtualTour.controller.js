// Backend/src/controllers/virtualTour.controller.js
import fetch from "node-fetch";
import { ApiError } from "../utils/ApiError.js";
import { ApiResponse } from "../utils/ApiResponse.js";
import { getRedisClient } from "../config/redis.config.js";

/**
 * Get historical places in Pakistan using Google Places API (v1)
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getHistoricalPlaces = async (req, res) => {
  try {
    const startTime = Date.now(); // For timing the response
    
    console.log(`[REQUEST] getHistoricalPlaces - Getting historical places in Pakistan`);

    // Generate a cache key
    const cacheKey = `historical:places:pakistan`;
    
    console.log(`[CACHE] Looking for key: ${cacheKey}`);
    
    // Try to get data from Redis Cloud cache with no expiry
    let cachedData;
    let dataSource = "API";
    
    try {
      console.time('redis-connect');
      const client = await getRedisClient();
      console.timeEnd('redis-connect');

      console.time('redis-get');
      cachedData = await client.get(cacheKey);
      console.timeEnd('redis-get');
      
      if (cachedData) {
        console.time('redis-parse');
        const parsedData = JSON.parse(cachedData);
        console.timeEnd('redis-parse');
        
        // Debug photo data in cached response
        if (parsedData.length > 0) {
          parsedData.forEach(place => {
            console.log(`[CACHE DEBUG] Place: ${place.displayName?.text}, Photos:`, 
              place.photos ? JSON.stringify(place.photos) : 'No photos');
          });
        }
        
        dataSource = "CACHE";
        console.log(`[CACHE HIT] Found data in Redis for key: ${cacheKey}`);
        console.log(`[CACHE] Retrieved ${parsedData.length} historical places from cache`);
        
        const responseTime = Date.now() - startTime;
        console.log(`[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Cache)`);
        
        return res.status(200).json(
          new ApiResponse(200, parsedData, "Historical places fetched from cache")
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

    // Historical places in Pakistan - predefined list of locations
    const historicalPlaces = [
      { name: "Lahore Fort", city: "Lahore" },
      { name: "Badshahi Mosque", city: "Lahore" },
      { name: "Wazir Khan Mosque", city: "Lahore" },
      { name: "Hiran Minar Park", city: "Sheikhupura" },
      { name: "Rohtas Fort", city: "Jhelum" },
      { name: "Makli Necropolis", city: "Thatta" },
      { name: "Ranikot Fort", city: "Jamshoro" },
      { name: "Altit Fort", city: "Hunza" },
      { name: "Derawar Fort", city: "Bahawalpur" }
    ];

    // Using the new Places API format for text search
    const url = "https://places.googleapis.com/v1/places:searchText";
    const placesData = [];
    
    // Process each historical place
    for (const place of historicalPlaces) {
      const searchQuery = `${place.name}, ${place.city}, Pakistan`;
      console.log(`[API REQUEST] Searching for: ${searchQuery}`);

      try {
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": apiKey,
            "X-Goog-FieldMask": "places.displayName,places.formattedAddress,places.location,places.rating,places.userRatingCount,places.currentOpeningHours,places.primaryTypeDisplayName,places.id,places.photos,places.editorialSummary,places.websiteUri,places.internationalPhoneNumber"
          },
          body: JSON.stringify({
            textQuery: searchQuery,
            languageCode: "en",
            maxResultCount: 1
          })
        });
        
        const data = await response.json();
        
        if (!response.ok) {
          console.error(`[API ERROR] Google Places API returned error for ${searchQuery}: ${JSON.stringify(data.error || {})}`);
          continue; // Skip to next place if there's an error
        }
        
        if (data.places && data.places.length > 0) {
          // Debug the photo structure
          console.log(`[PHOTO DEBUG] Photos for ${place.name}:`, JSON.stringify(data.places[0].photos || []));
          
          // Create a place object with all necessary data
          placesData.push({
            ...data.places[0],
            // Explicitly preserve the exact photo structure from the API
            photos: data.places[0].photos ? data.places[0].photos.map(photo => {
              // Ensure we keep the exact photo name string as received from the API
              return {
                name: photo.name, // Preserve the exact full reference path
                widthPx: photo.widthPx,
                heightPx: photo.heightPx
              };
            }) : [],
            historicalInfo: place
          });
        }
        
        // Add a small delay to avoid hitting rate limits
        await new Promise(resolve => setTimeout(resolve, 300));
      } catch (error) {
        console.error(`[ERROR] Failed to fetch data for ${searchQuery}: ${error.message}`);
        continue; // Skip to next place if there's an error
      }
    }

    console.log(`[API SUCCESS] Found ${placesData.length} historical places from Google Places API`);
    
    // Store in Redis Cloud cache with NO expiration
    if (placesData.length > 0) {
      try {
        const client = await getRedisClient();
        
        // Log the original photo format for debugging
        placesData.forEach(place => {
          console.log(`[PHOTO FORMAT] For ${place.displayName?.text}:`, 
            place.photos && place.photos.length > 0 ? place.photos[0].name : 'No photos');
        });
        
        // Store the complete place data to preserve all photo information
        const essentialPlaceData = placesData.map(place => ({
          id: place.id,
          displayName: place.displayName,
          formattedAddress: place.formattedAddress,
          location: place.location,
          currentOpeningHours: place.currentOpeningHours,
          internationalPhoneNumber: place.internationalPhoneNumber,
          // Ensure we're preserving the exact photo structure as-is
          photos: place.photos ? place.photos.map(photo => {
            // Ensure the name property is preserved exactly as received
            return {
              name: photo.name,
              widthPx: photo.widthPx,
              heightPx: photo.heightPx,
              // Include the place ID in the photo object to help with URL construction
              placeId: place.id
            };
          }) : [],
          rating: place.rating,
          userRatingCount: place.userRatingCount,
          primaryTypeDisplayName: place.primaryTypeDisplayName,
          editorialSummary: place.editorialSummary,
          websiteUri: place.websiteUri,
          historicalInfo: place.historicalInfo
        }));

        // Before storing in Redis, log the first photo for verification
        if (essentialPlaceData.length > 0 && essentialPlaceData[0].photos && essentialPlaceData[0].photos.length > 0) {
          console.log('[PHOTO VERIFICATION] Sample photo being stored:', 
            JSON.stringify(essentialPlaceData[0].photos[0]));
        }

        // Set with no expiration (passing no options means no expiry)
        await client.set(cacheKey, JSON.stringify(essentialPlaceData));
        
        // Verify that it's set correctly
        const ttl = await client.ttl(cacheKey);
        console.log(`[CACHE STORE] Successfully cached ${placesData.length} historical places with key: ${cacheKey} (TTL: ${ttl} seconds, -1 means no expiry)`);
      } catch (redisError) {
        console.error(`[REDIS ERROR] Failed to cache data: ${redisError.message}`);
        // Continue even if caching fails
      }
    } else {
      console.warn('[API WARNING] No historical places found to cache');
    }
    
    const responseTime = Date.now() - startTime;
    console.log(`[PERFORMANCE] Request completed in ${responseTime}ms (Source: Google Places API)`);
    
    return res.status(200).json(
      new ApiResponse(200, placesData, "Historical places fetched from Google API")
    );
  } catch (error) {
    console.error(`[ERROR] getHistoricalPlaces failed: ${error.message}`);
    if (error instanceof ApiError) {
      return res.status(error.statusCode).json(error);
    }
    return res.status(500).json(
      new ApiError(500, "Internal server error", [error.message])
    );
  }
};

/**
 * Clear the historical places cache (utility endpoint)
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const clearHistoricalPlacesCache = async (req, res) => {
  try {
    const cacheKey = `historical:places:pakistan`;
    const client = await getRedisClient();
    await client.del(cacheKey);
    
    return res.status(200).json(
      new ApiResponse(200, { cleared: true }, "Historical places cache cleared successfully")
    );
  } catch (error) {
    console.error(`[ERROR] Failed to clear cache: ${error.message}`);
    return res.status(500).json(
      new ApiError(500, "Failed to clear cache", [error.message])
    );
  }
};

/**
 * Get a specific historical place by ID
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getHistoricalPlaceById = async (req, res) => {
  try {
    const { placeId } = req.params;
    
    if (!placeId) {
      throw new ApiError(400, "Place ID is required");
    }
    
    // Try to get all places from cache first
    const client = await getRedisClient();
    const cachedData = await client.get(`historical:places:pakistan`);
    
    if (cachedData) {
      const places = JSON.parse(cachedData);
      const place = places.find(p => p.id === placeId);
      
      if (place) {
        return res.status(200).json(
          new ApiResponse(200, place, "Historical place fetched successfully")
        );
      }
    }
    
    // If not found in cache or cache is empty, fetch from API
    const apiKey = process.env.GOOGLE_PLACES_API_KEY;
    
    if (!apiKey) {
      throw new ApiError(500, "Google Places API key is not configured");
    }
    
    const url = `https://places.googleapis.com/v1/${placeId}`;
    
    const response = await fetch(url, {
      headers: {
        "X-Goog-Api-Key": apiKey,
        "X-Goog-FieldMask": "displayName,formattedAddress,location,rating,userRatingCount,currentOpeningHours,primaryTypeDisplayName,id,photos,editorialSummary,websiteUri,internationalPhoneNumber"
      }
    });
    
    const data = await response.json();
    
    if (!response.ok) {
      throw new ApiError(response.status, data.error?.message || "Failed to fetch place details");
    }
    
    return res.status(200).json(
      new ApiResponse(200, data, "Historical place fetched successfully")
    );
  } catch (error) {
    console.error(`[ERROR] getHistoricalPlaceById failed: ${error.message}`);
    if (error instanceof ApiError) {
      return res.status(error.statusCode).json(error);
    }
    return res.status(500).json(
      new ApiError(500, "Internal server error", [error.message])
    );
  }
};

// Updated getPhotoUrl function for VirtualTour.tsx

const getPhotoUrl = (place) => {
  if (!place.photos || place.photos.length === 0) {
    console.log("No photos for:", place.displayName?.text);
    return null;
  }

  const photo = place.photos[0];
  
  if (!photo || !photo.name) {
    console.log("Invalid photo data for:", place.displayName?.text);
    return null;
  }

  // Use a direct approach for all Google Places photos
  if (typeof photo.name === 'string') {
    // Handle complete paths (most reliable)
    if (photo.name.includes('places/') && photo.name.includes('/photos/')) {
      // Direct full reference - this should work for all formats including long references
      return `https://places.googleapis.com/v1/${photo.name}/media?key=${import.meta.env.VITE_GOOGLE_PLACES_API_KEY || ''}&maxHeightPx=800`;
    }
    
    // Only use place ID construction if we have a simple photo ID
    if (place.id && !photo.name.includes('/')) {
      return `https://places.googleapis.com/v1/places/${place.id}/photos/${photo.name}/media?key=${import.meta.env.VITE_GOOGLE_PLACES_API_KEY || ''}&maxHeightPx=800`;
    }
  }

  // Fallback to map image
  if (place.location) {
    return `https://maps.googleapis.com/maps/api/staticmap?center=${place.location.latitude},${place.location.longitude}&zoom=16&size=800x450&maptype=roadmap&markers=color:red%7C${place.location.latitude},${place.location.longitude}&key=${import.meta.env.VITE_GOOGLE_PLACES_API_KEY || ''}`;
  }
  
  return null;
};