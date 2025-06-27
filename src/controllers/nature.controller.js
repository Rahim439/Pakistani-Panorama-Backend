import fetch from "node-fetch";
import { ApiError } from "../utils/ApiError.js";
import { ApiResponse } from "../utils/ApiResponse.js";
import { getRedisClient } from "../config/redis.config.js";

/**
 * Get Northern Pakistan destinations using Google Places API with pagination
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getNorthernDestinations = async (req, res) => {
  try {
    const startTime = Date.now(); // For timing the response
    const { category = "all" } = req.query;

    console.log(`[REQUEST] getNorthernDestinations - category: ${category}`);

    // If it's not "all", handle with the existing category-specific logic
    if (category !== "all") {
      return await getSpecificCategoryDestinations(
        req,
        res,
        category,
        startTime
      );
    }

    // For "all" category, we'll fetch and combine data from all specific categories
    return await getAllCategoriesCombined(req, res, startTime);
  } catch (error) {
    console.error(`[ERROR] getNorthernDestinations failed: ${error.message}`);
    if (error instanceof ApiError) {
      return res.status(error.statusCode).json(error);
    }
    return res
      .status(500)
      .json(new ApiError(500, "Internal server error", [error.message]));
  }
};

/**
 * Get destinations for a specific category
 */
async function getSpecificCategoryDestinations(req, res, category, startTime) {
  // Generate a cache key based on category
  const cacheKey = `destinations:northern:${category}`;

  console.log(`[CACHE] Looking for key: ${cacheKey}`);

  // Try to get data from Redis cache
  let cachedData;

  try {
    console.time("redis-connect");
    const client = await getRedisClient();
    console.timeEnd("redis-connect");

    console.time("redis-get");
    cachedData = await client.get(cacheKey);
    console.timeEnd("redis-get");

    if (cachedData) {
      console.time("redis-parse");
      const parsedData = JSON.parse(cachedData);
      console.timeEnd("redis-parse");

      console.log(`[CACHE HIT] Found data in Redis for key: ${cacheKey}`);
      console.log(
        `[CACHE] Retrieved ${parsedData.length} destinations from cache`
      );

      // Log a sample of cached data to debug
      if (parsedData.length > 0) {
        console.log(
          `[CACHE DEBUG] First cached item has generativeSummary: ${!!parsedData[0].generativeSummary}`
        );
        console.log(
          `[CACHE DEBUG] First cached item has reviewSummary: ${!!parsedData[0].reviewSummary}`
        );
      }

      const responseTime = Date.now() - startTime;
      console.log(
        `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Cache)`
      );

      return res
        .status(200)
        .json(
          new ApiResponse(200, parsedData, "Destinations fetched from cache")
        );
    }
  } catch (redisError) {
    console.error(
      `[REDIS ERROR] Failed to get data from cache: ${redisError.message}`
    );
    // Continue with API call if Redis fails
  }

  console.log(
    `[CACHE MISS] No data in Redis for key: ${cacheKey}. Fetching from Google Places API...`
  );

  // Google Places API requires an API key
  const apiKey = process.env.GOOGLE_PLACES_API_KEY;

  if (!apiKey) {
    throw new ApiError(500, "Google Places API key is not configured");
  }

  // Define search parameters based on category
  let textQuery = `${category} in northern Pakistan`;

  // Location bounding box for northern Pakistan
  const locationRestriction = {
    rectangle: {
      low: { latitude: 30.0, longitude: 70.0 },
      high: { latitude: 37.5, longitude: 77.5 },
    },
  };

  // Using the Places API format for text search
  const url = "https://places.googleapis.com/v1/places:searchText";

  console.log(`[API REQUEST] Searching for: ${textQuery}`);

  try {
    // Fetch first page of results
    console.log(`[PAGINATION] Fetching first page of destinations`);
    const firstPageResponse = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": apiKey,
        "X-Goog-FieldMask":
          "places.displayName,places.formattedAddress,places.location,places.rating,places.userRatingCount,places.photos.name,places.photos.widthPx,places.photos.heightPx,places.editorialSummary,places.primaryTypeDisplayName,places.id,places.types,places.googleMapsUri,places.websiteUri,places.generativeSummary,places.reviewSummary,places.googleMapsLinks.reviewsUri,nextPageToken",
      },
      body: JSON.stringify({
        textQuery,
        locationRestriction,
        languageCode: "en",
        maxResultCount: 20,
      }),
    });

    const firstPageData = await firstPageResponse.json();

    if (!firstPageResponse.ok) {
      console.error(
        `[API ERROR] Google Places API returned error: ${JSON.stringify(firstPageData.error || {})}`
      );
      // Improved error handling for rate limiting
      if (firstPageResponse.status === 429) {
        return res
          .status(503)
          .json(
            new ApiError(
              503,
              "Google Places API rate limit exceeded, please try again later",
              [firstPageData.error || "Rate limit exceeded"]
            )
          );
      }
      throw new ApiError(
        firstPageResponse.status,
        "Error fetching destinations from Google API",
        [firstPageData.error || "Unknown error"]
      );
    }

    // Store all places in an array
    let allDestinations = firstPageData.places || [];
    console.log(
      `[PAGINATION] First page returned ${allDestinations.length} destinations`
    );

    // Log sample destination data for debugging
    if (allDestinations.length > 0) {
      const samplePlace = allDestinations[0];
      console.log(
        `[DEBUG] Sample destination: ${samplePlace.displayName?.text}`
      );
      console.log(
        `[DEBUG] Has generativeSummary: ${!!samplePlace.generativeSummary}`
      );
      console.log(`[DEBUG] Has reviewSummary: ${!!samplePlace.reviewSummary}`);

      if (samplePlace.generativeSummary) {
        console.log(
          `[DEBUG] Generative Summary structure:`,
          JSON.stringify(
            {
              hasOverview: !!samplePlace.generativeSummary.overview,
              hasOverviewText: !!samplePlace.generativeSummary.overview?.text,
              overview:
                samplePlace.generativeSummary.overview?.text?.substring(0, 50) +
                "...",
              hasDisclaimerText: !!samplePlace.generativeSummary.disclaimerText,
            },
            null,
            2
          )
        );
      }

      if (samplePlace.reviewSummary) {
        console.log(
          `[DEBUG] Review Summary:`,
          JSON.stringify(
            {
              text: samplePlace.reviewSummary.text?.substring(0, 50) + "...",
            },
            null,
            2
          )
        );
      }

      console.log(
        `[DEBUG] Has googleMapsLinks: ${!!samplePlace.googleMapsLinks}`
      );
      console.log(
        `[DEBUG] Has reviewsUri: ${!!samplePlace.googleMapsLinks?.reviewsUri}`
      );
    }

    // Check if we have a next page token
    if (firstPageData.nextPageToken) {
      console.log(
        `[PAGINATION] Next page token found: ${firstPageData.nextPageToken.substring(0, 20)}...`
      );

      // Google requires a small delay before using the nextPageToken
      await new Promise((resolve) => setTimeout(resolve, 2500));

      // Fetch second page of results
      console.log(`[PAGINATION] Fetching second page of destinations`);
      try {
        const secondPageResponse = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": apiKey,
            "X-Goog-FieldMask":
              "places.displayName,places.formattedAddress,places.location,places.rating,places.userRatingCount,places.photos.name,places.photos.widthPx,places.photos.heightPx,places.editorialSummary,places.primaryTypeDisplayName,places.id,places.types,places.googleMapsUri,places.websiteUri,places.generativeSummary,places.reviewSummary,places.googleMapsLinks.reviewsUri",
          },
          body: JSON.stringify({
            pageToken: firstPageData.nextPageToken,
          }),
        });

        const secondPageData = await secondPageResponse.json();

        if (secondPageResponse.ok && secondPageData.places) {
          console.log(
            `[PAGINATION] Second page returned ${secondPageData.places.length} destinations`
          );

          // Log second page sample if available
          if (secondPageData.places.length > 0) {
            const sampleSecondPlace = secondPageData.places[0];
            console.log(
              `[DEBUG] Second page sample destination: ${sampleSecondPlace.displayName?.text}`
            );
            console.log(
              `[DEBUG] Has generativeSummary: ${!!sampleSecondPlace.generativeSummary}`
            );
            console.log(
              `[DEBUG] Has reviewSummary: ${!!sampleSecondPlace.reviewSummary}`
            );
          }

          allDestinations = [...allDestinations, ...secondPageData.places];
        } else {
          console.error(
            `[PAGINATION ERROR] Failed to fetch second page: ${JSON.stringify(secondPageData.error || {})}`
          );
          // Continue with just the first page results
        }
      } catch (paginationError) {
        console.error(
          `[PAGINATION ERROR] Error fetching second page: ${paginationError.message}`
        );
        // Continue with just the first page results
      }
    } else {
      console.log(`[PAGINATION] No more pages available`);
    }

    console.log(
      `[API SUCCESS] Found total of ${allDestinations.length} destinations from Google Places API`
    );

    // Filter results for the specific category
    const originalCount = allDestinations.length;
    allDestinations = filterDestinationsByCategory(allDestinations, category);
    console.log(
      `[FILTER] Filtered from ${originalCount} to ${allDestinations.length} destinations for category: ${category}`
    );

    // Store in Redis cache with NO expiration
    if (allDestinations.length > 0) {
      try {
        const client = await getRedisClient();

        // Store essential place data
        const essentialData = prepareDestinationsForCache(allDestinations);

        // Set with NO expiration by not passing any EX option
        await client.set(cacheKey, JSON.stringify(essentialData));

        const ttl = await client.ttl(cacheKey);
        console.log(
          `[CACHE STORE] Successfully cached ${essentialData.length} destinations with key: ${cacheKey} (TTL: ${ttl}, -1 means no expiry)`
        );
      } catch (redisError) {
        console.error(
          `[REDIS ERROR] Failed to cache data: ${redisError.message}`
        );
        // Continue even if caching fails
      }
    } else {
      console.log(
        `[CACHE SKIP] No destinations found, skipping cache storage for key: ${cacheKey}`
      );
    }

    const responseTime = Date.now() - startTime;
    console.log(
      `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Google Places API)`
    );

    return res
      .status(200)
      .json(
        new ApiResponse(
          200,
          allDestinations,
          `${category} destinations fetched from Google API`
        )
      );
  } catch (fetchError) {
    console.error(
      `[FETCH ERROR] Failed to get data from Google API: ${fetchError.message}`
    );
    throw fetchError;
  }
}

/**
 * Get and combine destinations from all categories
 */
async function getAllCategoriesCombined(req, res, startTime) {
  const categories = [
    "mountains",
    "valleys",
    "lakes",
    "glaciers",
    "waterfalls",
    "caves",
  ];
  const allCacheKey = `destinations:northern:all`;

  console.log(
    `[CACHE] Looking for combined destinations with key: ${allCacheKey}`
  );

  // Try to get combined data from cache first
  try {
    const client = await getRedisClient();
    const cachedData = await client.get(allCacheKey);

    if (cachedData) {
      const parsedData = JSON.parse(cachedData);
      console.log(
        `[CACHE HIT] Found all categories data in Redis with ${parsedData.length} destinations`
      );

      // Log a sample of cached data
      if (parsedData.length > 0) {
        console.log(
          `[CACHE DEBUG] Combined cache has generativeSummary: ${!!parsedData[0].generativeSummary}`
        );
        console.log(
          `[CACHE DEBUG] Combined cache has reviewSummary: ${!!parsedData[0].reviewSummary}`
        );
      }

      const responseTime = Date.now() - startTime;
      console.log(
        `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Cache)`
      );

      return res
        .status(200)
        .json(
          new ApiResponse(
            200,
            parsedData,
            "All destinations fetched from cache"
          )
        );
    }
  } catch (redisError) {
    console.error(
      `[REDIS ERROR] Failed to get all data from cache: ${redisError.message}`
    );
  }

  // If not in cache, fetch each category and combine
  console.log(`[CACHE MISS] Fetching data for all categories...`);
  let allDestinations = [];
  const apiKey = process.env.GOOGLE_PLACES_API_KEY;

  if (!apiKey) {
    throw new ApiError(500, "Google Places API key is not configured");
  }

  for (const cat of categories) {
    // Try to get from category-specific cache first
    const catCacheKey = `destinations:northern:${cat}`;
    let categoryDestinations = [];

    try {
      // First check if category data is in cache
      const client = await getRedisClient();
      const catCachedData = await client.get(catCacheKey);

      if (catCachedData) {
        console.log(`[CACHE HIT] Using cached data for ${cat}`);
        categoryDestinations = JSON.parse(catCachedData);

        // Log sample summary fields from cache
        if (categoryDestinations.length > 0) {
          console.log(
            `[DEBUG] ${cat} cache has generativeSummary: ${!!categoryDestinations[0].generativeSummary}`
          );
          console.log(
            `[DEBUG] ${cat} cache has reviewSummary: ${!!categoryDestinations[0].reviewSummary}`
          );
        }
      } else {
        // If not in cache, fetch from API
        console.log(`[CACHE MISS] Fetching ${cat} from API`);

        // Location bounding box for northern Pakistan
        const locationRestriction = {
          rectangle: {
            low: { latitude: 30.0, longitude: 70.0 },
            high: { latitude: 37.5, longitude: 77.5 },
          },
        };

        const textQuery = `${cat} in northern Pakistan`;
        const url = "https://places.googleapis.com/v1/places:searchText";

        // Fetch first page
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": apiKey,
            "X-Goog-FieldMask":
              "places.displayName,places.formattedAddress,places.location,places.rating,places.userRatingCount,places.photos.name,places.photos.widthPx,places.photos.heightPx,places.editorialSummary,places.primaryTypeDisplayName,places.id,places.types,places.googleMapsUri,places.websiteUri,places.generativeSummary,places.reviewSummary,places.googleMapsLinks.reviewsUri,nextPageToken",
          },
          body: JSON.stringify({
            textQuery,
            locationRestriction,
            languageCode: "en",
            maxResultCount: 20,
          }),
        });

        const data = await response.json();

        if (!response.ok) {
          console.error(
            `[API ERROR] Failed to fetch ${cat}: ${JSON.stringify(data.error || {})}`
          );
          // Skip this category on error, but continue with others
          continue;
        }

        // Filter and process results
        categoryDestinations = data.places || [];

        // Log sample from API for debugging
        if (categoryDestinations.length > 0) {
          const samplePlace = categoryDestinations[0];
          console.log(
            `[DEBUG] ${cat} API sample destination: ${samplePlace.displayName?.text}`
          );
          console.log(
            `[DEBUG] Has generativeSummary: ${!!samplePlace.generativeSummary}`
          );
          console.log(
            `[DEBUG] Has reviewSummary: ${!!samplePlace.reviewSummary}`
          );

          if (samplePlace.generativeSummary) {
            console.log(
              `[DEBUG] ${cat} Generative Summary structure:`,
              JSON.stringify(
                {
                  hasOverview: !!samplePlace.generativeSummary.overview,
                  hasOverviewText:
                    !!samplePlace.generativeSummary.overview?.text,
                  overview:
                    samplePlace.generativeSummary.overview?.text?.substring(
                      0,
                      50
                    ) + "...",
                },
                null,
                2
              )
            );
          }
        }

        categoryDestinations = filterDestinationsByCategory(
          categoryDestinations,
          cat
        );

        // Cache the category results
        if (categoryDestinations.length > 0) {
          const essentialData =
            prepareDestinationsForCache(categoryDestinations);
          await client.set(catCacheKey, JSON.stringify(essentialData));
          console.log(
            `[CACHE STORE] Cached ${essentialData.length} ${cat} destinations`
          );
        }
      }

      // Add to combined results, ensuring no duplicates by ID
      if (categoryDestinations.length > 0) {
        console.log(
          `[COMBINE] Adding ${categoryDestinations.length} ${cat} destinations`
        );
        categoryDestinations.forEach((destination) => {
          if (!allDestinations.some((d) => d.id === destination.id)) {
            allDestinations.push(destination);
          }
        });
      }
    } catch (error) {
      console.error(`[ERROR] Failed to fetch ${cat}: ${error.message}`);
      // Continue with other categories even if one fails
    }
  }

  console.log(
    `[COMBINE] Total combined destinations: ${allDestinations.length}`
  );

  // Randomize order for more interesting results
  allDestinations.sort(() => Math.random() - 0.5);

  // Save combined results to the "all" cache
  if (allDestinations.length > 0) {
    try {
      const client = await getRedisClient();
      await client.set(allCacheKey, JSON.stringify(allDestinations));
      console.log(
        `[CACHE STORE] Cached combined results with ${allDestinations.length} destinations`
      );
    } catch (redisError) {
      console.error(
        `[REDIS ERROR] Failed to cache combined data: ${redisError.message}`
      );
    }
  }

  const responseTime = Date.now() - startTime;
  console.log(
    `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Combined Categories)`
  );

  return res
    .status(200)
    .json(
      new ApiResponse(
        200,
        allDestinations,
        "All destinations fetched by combining categories"
      )
    );
}

/**
 * Filter destinations by category
 */
function filterDestinationsByCategory(destinations, category) {
  if (!destinations || destinations.length === 0) {
    return [];
  }

  // Helper function to check if a place matches a category
  const matchesCategory = (place, categoryName, keywords) => {
    // Check if it's a natural feature or if the name contains any of the keywords
    const isNaturalFeature =
      place.types && place.types.includes("natural_feature");
    const displayName = place.displayName?.text.toLowerCase() || "";

    return (
      isNaturalFeature ||
      keywords.some((keyword) => displayName.includes(keyword))
    );
  };

  // Filter based on category
  switch (category) {
    case "mountains":
      return destinations.filter((place) =>
        matchesCategory(place, "mountains", [
          "mountain",
          "peak",
          "hill",
          "mount",
        ])
      );
    case "valleys":
      return destinations.filter((place) =>
        matchesCategory(place, "valleys", ["valley", "vale", "glen"])
      );
    case "lakes":
      return destinations.filter((place) =>
        matchesCategory(place, "lakes", ["lake", "reservoir", "pond", "water"])
      );
    case "glaciers":
      return destinations.filter((place) =>
        matchesCategory(place, "glaciers", ["glacier", "ice", "snow field"])
      );
    case "waterfalls":
      return destinations.filter((place) =>
        matchesCategory(place, "waterfalls", ["waterfall", "falls", "cascade"])
      );
    case "caves":
      return destinations.filter((place) =>
        matchesCategory(place, "caves", ["cave", "cavern", "grotto"])
      );
    default:
      return destinations;
  }
}

/**
 * Prepare destinations data for caching
 */
function prepareDestinationsForCache(destinations) {
  // Log first place's summaries if available for debugging
  if (destinations && destinations.length > 0) {
    const firstPlace = destinations[0];
    console.log(`[CACHE PREP] Destination: ${firstPlace.displayName?.text}`);
    console.log(
      `[CACHE PREP] Has generativeSummary: ${!!firstPlace.generativeSummary}`
    );
    console.log(
      `[CACHE PREP] Has reviewSummary: ${!!firstPlace.reviewSummary}`
    );

    if (firstPlace.generativeSummary) {
      console.log(
        `[CACHE PREP] Generative summary structure:`,
        JSON.stringify(
          {
            hasOverview: !!firstPlace.generativeSummary.overview,
            hasOverviewText: !!firstPlace.generativeSummary.overview?.text,
            hasDisclaimerText: !!firstPlace.generativeSummary.disclaimerText,
          },
          null,
          2
        )
      );
    }
  }

  return destinations.map((place) => {
    // Check each place for summary fields and log any inconsistencies
    if (place.generativeSummary && !place.generativeSummary.overview) {
      console.log(
        `[WARNING] Place ${place.id} has generativeSummary but missing overview property`
      );
    }

    return {
      id: place.id,
      displayName: place.displayName,
      formattedAddress: place.formattedAddress,
      location: place.location,
      photos: place.photos
        ? place.photos.map((photo) => ({
            name: photo.name,
            widthPx: photo.widthPx,
            heightPx: photo.heightPx,
          }))
        : [],
      rating: place.rating,
      userRatingCount: place.userRatingCount,
      editorialSummary: place.editorialSummary,
      primaryTypeDisplayName: place.primaryTypeDisplayName,
      types: place.types,
      googleMapsUri: place.googleMapsUri,
      websiteUri: place.websiteUri,
      // Preserve exact structure of these fields without modification
      generativeSummary: place.generativeSummary,
      reviewSummary: place.reviewSummary,
      googleMapsLinks: {
        reviewsUri: place.googleMapsLinks?.reviewsUri || null,
      },
    };
  });
}
