import fetch from "node-fetch";
import { ApiError } from "../utils/ApiError.js";
import { ApiResponse } from "../utils/ApiResponse.js";
import { getRedisClient } from "../config/redis.config.js";

// HELPER FUNCTIONS FOR CHUNKED REDIS STORAGE - REMOVED ALL EXPIRY TIMES
const storeDataInChunks = async (client, baseKey, data, chunkSize = 50) => {
  const pipeline = client.multi();

  // Store metadata
  const metadata = {
    totalItems: data.length,
    chunks: Math.ceil(data.length / chunkSize),
    lastUpdated: new Date().toISOString(),
    chunkSize: chunkSize,
  };

  // Removed EX: 86400 expiration
  pipeline.set(`${baseKey}:meta`, JSON.stringify(metadata));

  // Store chunks
  for (let i = 0; i < data.length; i += chunkSize) {
    const chunk = data.slice(i, i + chunkSize);
    const chunkKey = `${baseKey}:chunk:${Math.floor(i / chunkSize)}`;
    // Removed EX: 86400 expiration
    pipeline.set(chunkKey, JSON.stringify(chunk));
  }

  await pipeline.exec();
  console.log(
    `[CACHE] Stored ${data.length} items in ${metadata.chunks} chunks for ${baseKey}`
  );
  return metadata;
};

const getDataFromChunks = async (client, baseKey, page, limit) => {
  try {
    // Get metadata
    const metaStr = await client.get(`${baseKey}:meta`);
    if (!metaStr) return null;

    const meta = JSON.parse(metaStr);
    const { totalItems, chunks, chunkSize } = meta;

    // Calculate which chunks we need for this page
    const startIndex = (page - 1) * limit;
    const endIndex = Math.min(startIndex + limit, totalItems);

    if (startIndex >= totalItems) {
      return {
        items: [],
        pagination: {
          totalItems,
          itemsPerPage: limit,
          currentPage: page,
          totalPages: Math.ceil(totalItems / limit),
          hasNextPage: false,
          hasPrevPage: page > 1,
        },
      };
    }

    const startChunk = Math.floor(startIndex / chunkSize);
    const endChunk = Math.floor((endIndex - 1) / chunkSize);

    // Fetch only needed chunks in parallel
    const chunkPromises = [];
    for (let i = startChunk; i <= Math.min(endChunk, chunks - 1); i++) {
      chunkPromises.push(client.get(`${baseKey}:chunk:${i}`));
    }

    console.time("redis-get-chunks");
    const chunkResults = await Promise.all(chunkPromises);
    console.timeEnd("redis-get-chunks");

    // Combine chunks
    let allData = [];
    chunkResults.forEach((chunk) => {
      if (chunk) {
        allData = allData.concat(JSON.parse(chunk));
      }
    });

    // Extract only needed items for this page
    const offset = startIndex - startChunk * chunkSize;
    const paginatedItems = allData.slice(offset, offset + limit);

    return {
      items: paginatedItems,
      pagination: {
        totalItems,
        itemsPerPage: limit,
        currentPage: page,
        totalPages: Math.ceil(totalItems / limit),
        hasNextPage: page < Math.ceil(totalItems / limit),
        hasPrevPage: page > 1,
      },
    };
  } catch (error) {
    console.error(`[CHUNK ERROR] Failed to get chunked data: ${error.message}`);
    return null;
  }
};

const convertToChunkedStorage = async (client, baseKey, fullDataset) => {
  try {
    console.log(`[CONVERSION] Converting ${baseKey} to chunked storage...`);

    // Store in chunked format
    await storeDataInChunks(client, baseKey, fullDataset);

    // Delete the original large key
    await client.del(baseKey);
    console.log(
      `[CONVERSION] Successfully converted ${baseKey} to chunked storage`
    );

    return true;
  } catch (error) {
    console.error(
      `[CONVERSION ERROR] Failed to convert to chunked storage: ${error.message}`
    );
    return false;
  }
};

// HELPER FUNCTION TO FETCH DATA FROM MULTIPLE QUERIES
const fetchMultipleQueries = async (
  queries,
  apiKey,
  url,
  fieldMask,
  targetCount,
  category
) => {
  const uniquePlacesMap = new Map();
  let totalApiCalls = 0;

  for (
    let queryIndex = 0;
    queryIndex < queries.length && uniquePlacesMap.size < targetCount;
    queryIndex++
  ) {
    const textQuery = queries[queryIndex];
    console.log(
      `[MULTI-QUERY] ${category} - Processing query ${queryIndex + 1}/${queries.length}: "${textQuery}"`
    );

    try {
      // Fetch first page for this query
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Goog-Api-Key": apiKey,
          "X-Goog-FieldMask": fieldMask,
        },
        body: JSON.stringify({
          textQuery,
          languageCode: "en",
          maxResultCount: 20,
        }),
      });

      totalApiCalls++;
      const data = await response.json();

      if (!response.ok) {
        console.error(
          `[API ERROR] Query "${textQuery}" failed: ${JSON.stringify(data.error || {})}`
        );
        continue; // Skip to next query
      }

      // Add first page results to the map
      const firstPagePlaces = data.places || [];
      let newUniqueCount = 0;

      firstPagePlaces.forEach((place) => {
        if (!uniquePlacesMap.has(place.id)) {
          uniquePlacesMap.set(place.id, place);
          newUniqueCount++;
        }
      });

      console.log(
        `[MULTI-QUERY] Query "${textQuery}" - First page: ${firstPagePlaces.length} places, ${newUniqueCount} new unique. Total unique: ${uniquePlacesMap.size}`
      );

      // Check for pagination and fetch more pages if needed
      let pageCounter = 1;
      let nextPageToken = data.nextPageToken;

      while (
        nextPageToken &&
        pageCounter < 5 &&
        uniquePlacesMap.size < targetCount
      ) {
        console.log(
          `[PAGINATION] Query "${textQuery}" - Fetching page ${pageCounter + 1}`
        );

        // Google requires a delay before using nextPageToken
        await new Promise((resolve) => setTimeout(resolve, 2500));

        try {
          const nextPageResponse = await fetch(url, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-Goog-Api-Key": apiKey,
              "X-Goog-FieldMask": fieldMask,
            },
            body: JSON.stringify({
              textQuery,
              languageCode: "en",
              maxResultCount: 20,
              pageToken: nextPageToken,
            }),
          });

          totalApiCalls++;
          const nextPageData = await nextPageResponse.json();

          if (!nextPageResponse.ok) {
            console.error(
              `[PAGINATION ERROR] Query "${textQuery}" page ${pageCounter + 1} failed`
            );
            break;
          }

          const nextPagePlaces = nextPageData.places || [];
          let pageNewUniqueCount = 0;

          nextPagePlaces.forEach((place) => {
            if (!uniquePlacesMap.has(place.id)) {
              uniquePlacesMap.set(place.id, place);
              pageNewUniqueCount++;
            }
          });

          console.log(
            `[PAGINATION] Query "${textQuery}" page ${pageCounter + 1}: ${nextPagePlaces.length} places, ${pageNewUniqueCount} new unique. Total unique: ${uniquePlacesMap.size}`
          );

          if (nextPagePlaces.length === 0 || pageNewUniqueCount === 0) {
            break;
          }

          nextPageToken = nextPageData.nextPageToken;
          pageCounter++;

          if (!nextPageToken) {
            break;
          }
        } catch (paginationError) {
          console.error(
            `[PAGINATION ERROR] Query "${textQuery}" page ${pageCounter + 1}: ${paginationError.message}`
          );
          break;
        }
      }

      // Add delay between queries to respect rate limits
      if (queryIndex < queries.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    } catch (queryError) {
      console.error(
        `[QUERY ERROR] Failed to process query "${textQuery}": ${queryError.message}`
      );
      continue;
    }
  }

  console.log(
    `[MULTI-QUERY COMPLETE] ${category} - Total API calls: ${totalApiCalls}, Unique places found: ${uniquePlacesMap.size}`
  );
  return Array.from(uniquePlacesMap.values());
};

/**
 * Get hotels in Pakistan using Google Places API with pagination
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getHotels = async (req, res) => {
  try {
    const startTime = Date.now();
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const targetUniqueCount = 200; // Target to fetch 200 unique hotels

    console.log(
      `[REQUEST] getHotels - Getting hotels in Pakistan (page ${page}, limit ${limit})`
    );

    const baseCacheKey = `attractions:hotels:pakistan`;
    const paginatedCacheKey = `${baseCacheKey}:page${page}:limit${limit}`;

    try {
      console.time("redis-connect");
      const client = await getRedisClient();
      console.timeEnd("redis-connect");

      // 1. Try paginated cache first (fastest)
      console.time("redis-get");
      let cachedData = await client.get(paginatedCacheKey);
      console.timeEnd("redis-get");

      if (cachedData) {
        console.time("redis-parse");
        const parsedData = JSON.parse(cachedData);
        console.timeEnd("redis-parse");

        console.log(
          `[CACHE HIT] Found paginated data in Redis: ${paginatedCacheKey}`
        );
        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Paginated Cache)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              parsedData,
              "Hotels fetched from paginated cache"
            )
          );
      }

      // 2. Try chunked storage (fast)
      console.log(`[CACHE] Trying chunked storage for ${baseCacheKey}...`);
      const chunkedData = await getDataFromChunks(
        client,
        baseCacheKey,
        page,
        limit
      );

      if (chunkedData) {
        console.log(`[CACHE HIT] Found chunked data in Redis`);

        const responseData = {
          hotels: chunkedData.items,
          pagination: chunkedData.pagination,
        };

        // Cache this paginated result for faster access next time - removed expiry
        try {
          await client.set(paginatedCacheKey, JSON.stringify(responseData));
          console.log(
            `[CACHE STORE] Stored paginated hotels data with key: ${paginatedCacheKey}`
          );
        } catch (cacheError) {
          console.error(
            `[CACHE ERROR] Failed to store paginated data: ${cacheError.message}`
          );
        }

        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Chunked Cache)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              responseData,
              "Hotels fetched from chunked cache"
            )
          );
      }

      // 3. Fallback to full dataset (will convert to chunks if found)
      console.log(
        `[CACHE] Trying full dataset for conversion: ${baseCacheKey}`
      );
      console.time("redis-get-full");
      const fullDataset = await client.get(baseCacheKey);
      console.timeEnd("redis-get-full");

      if (fullDataset) {
        console.log(
          `[CACHE HIT] Found full dataset in Redis. Converting to chunked storage...`
        );

        console.time("redis-parse-full");
        const allHotels = JSON.parse(fullDataset);
        console.timeEnd("redis-parse-full");

        // Convert to chunked storage for future requests
        await convertToChunkedStorage(client, baseCacheKey, allHotels);

        // Apply pagination in memory for this request
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedHotels = allHotels.slice(startIndex, endIndex);

        const paginationInfo = {
          totalItems: allHotels.length,
          itemsPerPage: limit,
          currentPage: page,
          totalPages: Math.ceil(allHotels.length / limit),
          hasNextPage: page < Math.ceil(allHotels.length / limit),
          hasPrevPage: page > 1,
        };

        const responseData = {
          hotels: paginatedHotels,
          pagination: paginationInfo,
        };

        // Store paginated result in cache - removed expiry
        await client.set(paginatedCacheKey, JSON.stringify(responseData));

        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Full Dataset + Conversion)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              responseData,
              "Hotels fetched from cache and optimized for future requests"
            )
          );
      }
    } catch (redisError) {
      console.error(
        `[REDIS ERROR] Failed to get data from cache: ${redisError.message}`
      );
    }

    // If we get here, need to fetch from Google Places API
    console.log(
      `[CACHE MISS] No data in Redis. Fetching from Google Places API...`
    );

    const apiKey = process.env.GOOGLE_PLACES_API_KEY;
    if (!apiKey) {
      throw new ApiError(500, "Google Places API key is not configured");
    }

    const url = "https://places.googleapis.com/v1/places:searchText";

    // Multiple queries for better hotel coverage across Pakistan
    const hotelQueries = [
      "5 star hotels in Karachi Pakistan",
      "best hotels in Lahore Pakistan",
      "luxury hotels in Islamabad Pakistan",
      "hotels in Peshawar Pakistan",
      "hotels in Quetta Pakistan",
      "hotels in Multan Pakistan",
      "hotels in Faisalabad Pakistan",
      "budget hotels in Pakistan",
      "business hotels in Pakistan",
      "resorts in Northern Pakistan",
      "hotels in Rawalpindi Pakistan",
      "hotels in Hyderabad Pakistan",
      "hotels in Gujranwala Pakistan",
      "hotels in Sialkot Pakistan",
      "heritage hotels in Pakistan",
    ];

    const fieldMask =
      "places.displayName,places.formattedAddress,places.location,places.rating,places.userRatingCount,places.photos.name,places.photos.widthPx,places.photos.heightPx,places.editorialSummary,places.primaryTypeDisplayName,places.id,places.types,places.googleMapsUri,places.websiteUri,places.priceLevel,places.currentOpeningHours,places.internationalPhoneNumber,nextPageToken";

    try {
      // Fetch from multiple queries
      const allHotels = await fetchMultipleQueries(
        hotelQueries,
        apiKey,
        url,
        fieldMask,
        targetUniqueCount,
        "Hotels"
      );

      console.log(
        `[API SUCCESS] Total unique hotels fetched: ${allHotels.length}`
      );

      if (allHotels.length > 0) {
        try {
          const client = await getRedisClient();

          const essentialData = allHotels.map((hotel) => ({
            id: hotel.id,
            displayName: hotel.displayName,
            formattedAddress: hotel.formattedAddress,
            location: hotel.location,
            photos: hotel.photos
              ? hotel.photos.map((photo) => ({
                  name: photo.name,
                  widthPx: photo.widthPx,
                  heightPx: photo.heightPx,
                }))
              : [],
            rating: hotel.rating,
            userRatingCount: hotel.userRatingCount,
            editorialSummary: hotel.editorialSummary,
            primaryTypeDisplayName: hotel.primaryTypeDisplayName,
            priceLevel: hotel.priceLevel,
            websiteUri: hotel.websiteUri,
            googleMapsUri: hotel.googleMapsUri,
            currentOpeningHours: hotel.currentOpeningHours,
            internationalPhoneNumber: hotel.internationalPhoneNumber,
          }));

          // Store using chunked method instead of single large key
          await storeDataInChunks(client, baseCacheKey, essentialData);
          console.log(
            `[CACHE STORE] Stored hotels data using chunked approach`
          );

          // Calculate pagination for response
          const startIndex = (page - 1) * limit;
          const endIndex = startIndex + limit;
          const paginatedHotels = essentialData.slice(startIndex, endIndex);

          const paginationInfo = {
            totalItems: essentialData.length,
            itemsPerPage: limit,
            currentPage: page,
            totalPages: Math.ceil(essentialData.length / limit),
            hasNextPage: page < Math.ceil(essentialData.length / limit),
            hasPrevPage: page > 1,
          };

          const responseData = {
            hotels: paginatedHotels,
            pagination: paginationInfo,
          };

          // Store paginated data - removed expiry
          await client.set(paginatedCacheKey, JSON.stringify(responseData));

          const responseTime = Date.now() - startTime;
          console.log(
            `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Google API with chunked storage)`
          );

          return res
            .status(200)
            .json(
              new ApiResponse(
                200,
                responseData,
                "Hotels fetched from Google API"
              )
            );
        } catch (cacheError) {
          console.error(
            `[CACHE ERROR] Failed to store data: ${cacheError.message}`
          );
        }
      }

      return res.status(200).json(
        new ApiResponse(
          200,
          {
            hotels: [],
            pagination: { totalItems: 0, currentPage: page, totalPages: 0 },
          },
          "No hotels found"
        )
      );
    } catch (fetchError) {
      console.error(
        `[FETCH ERROR] Failed to get data from Google API: ${fetchError.message}`
      );
      throw fetchError;
    }
  } catch (error) {
    console.error(`[ERROR] getHotels failed: ${error.message}`);
    if (error instanceof ApiError) {
      return res.status(error.statusCode).json(error);
    }
    return res
      .status(500)
      .json(new ApiError(500, "Internal server error", [error.message]));
  }
};

/**
 * Get restaurants in Pakistan using Google Places API with pagination
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getRestaurants = async (req, res) => {
  try {
    const startTime = Date.now();
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const targetUniqueCount = 200; // Target to fetch 200 unique restaurants

    console.log(
      `[REQUEST] getRestaurants - Getting restaurants in Pakistan (page ${page}, limit ${limit})`
    );

    const baseCacheKey = `attractions:restaurants:pakistan`;
    const paginatedCacheKey = `${baseCacheKey}:page${page}:limit${limit}`;

    try {
      console.time("redis-connect");
      const client = await getRedisClient();
      console.timeEnd("redis-connect");

      // 1. Try paginated cache first (fastest)
      console.time("redis-get");
      let cachedData = await client.get(paginatedCacheKey);
      console.timeEnd("redis-get");

      if (cachedData) {
        console.time("redis-parse");
        const parsedData = JSON.parse(cachedData);
        console.timeEnd("redis-parse");

        console.log(
          `[CACHE HIT] Found paginated data in Redis: ${paginatedCacheKey}`
        );
        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Paginated Cache)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              parsedData,
              "Restaurants fetched from paginated cache"
            )
          );
      }

      // 2. Try chunked storage (fast)
      console.log(`[CACHE] Trying chunked storage for ${baseCacheKey}...`);
      const chunkedData = await getDataFromChunks(
        client,
        baseCacheKey,
        page,
        limit
      );

      if (chunkedData) {
        console.log(`[CACHE HIT] Found chunked data in Redis`);

        const responseData = {
          restaurants: chunkedData.items,
          pagination: chunkedData.pagination,
        };

        // Cache this paginated result - removed expiry
        try {
          await client.set(paginatedCacheKey, JSON.stringify(responseData));
          console.log(
            `[CACHE STORE] Stored paginated restaurants data with key: ${paginatedCacheKey}`
          );
        } catch (cacheError) {
          console.error(
            `[CACHE ERROR] Failed to store paginated data: ${cacheError.message}`
          );
        }

        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Chunked Cache)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              responseData,
              "Restaurants fetched from chunked cache"
            )
          );
      }

      // 3. Fallback to full dataset (will convert to chunks if found)
      console.log(
        `[CACHE] Trying full dataset for conversion: ${baseCacheKey}`
      );
      console.time("redis-get-full");
      const fullDataset = await client.get(baseCacheKey);
      console.timeEnd("redis-get-full");

      if (fullDataset) {
        console.log(
          `[CACHE HIT] Found full dataset in Redis. Converting to chunked storage...`
        );

        console.time("redis-parse-full");
        const allRestaurants = JSON.parse(fullDataset);
        console.timeEnd("redis-parse-full");

        // Convert to chunked storage
        await convertToChunkedStorage(client, baseCacheKey, allRestaurants);

        // Apply pagination for this request
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedRestaurants = allRestaurants.slice(startIndex, endIndex);

        const paginationInfo = {
          totalItems: allRestaurants.length,
          itemsPerPage: limit,
          currentPage: page,
          totalPages: Math.ceil(allRestaurants.length / limit),
          hasNextPage: page < Math.ceil(allRestaurants.length / limit),
          hasPrevPage: page > 1,
        };

        const responseData = {
          restaurants: paginatedRestaurants,
          pagination: paginationInfo,
        };

        // Removed expiry
        await client.set(paginatedCacheKey, JSON.stringify(responseData));

        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Full Dataset + Conversion)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              responseData,
              "Restaurants fetched from cache and optimized for future requests"
            )
          );
      }
    } catch (redisError) {
      console.error(
        `[REDIS ERROR] Failed to get data from cache: ${redisError.message}`
      );
    }

    // Google Places API fetch logic
    console.log(
      `[CACHE MISS] No data in Redis. Fetching from Google Places API...`
    );

    const apiKey = process.env.GOOGLE_PLACES_API_KEY;
    if (!apiKey) {
      throw new ApiError(500, "Google Places API key is not configured");
    }

    const url = "https://places.googleapis.com/v1/places:searchText";

    // Multiple queries for comprehensive restaurant coverage
    const restaurantQueries = [
      "best restaurants in Karachi Pakistan",
      "top rated restaurants in Lahore Pakistan",
      "fine dining in Islamabad Pakistan",
      "traditional Pakistani restaurants",
      "BBQ restaurants in Pakistan",
      "seafood restaurants in Pakistan",
      "fast food restaurants in Pakistan",
      "street food in Pakistan",
      "affordable restaurants in Pakistan",
      "international cuisine in Pakistan",
      "restaurants in Peshawar Pakistan",
      "restaurants in Quetta Pakistan",
      "restaurants in Multan Pakistan",
      "restaurants in Faisalabad Pakistan",
      "buffet restaurants in Pakistan",
    ];

    const fieldMask =
      "places.displayName,places.formattedAddress,places.location,places.rating,places.userRatingCount,places.photos.name,places.photos.widthPx,places.photos.heightPx,places.editorialSummary,places.primaryTypeDisplayName,places.id,places.types,places.googleMapsUri,places.websiteUri,places.priceLevel,places.currentOpeningHours,places.internationalPhoneNumber,nextPageToken";

    try {
      // Fetch from multiple queries
      const allRestaurants = await fetchMultipleQueries(
        restaurantQueries,
        apiKey,
        url,
        fieldMask,
        targetUniqueCount,
        "Restaurants"
      );

      console.log(
        `[API SUCCESS] Total unique restaurants fetched: ${allRestaurants.length}`
      );

      if (allRestaurants.length > 0) {
        try {
          const client = await getRedisClient();

          const essentialData = allRestaurants.map((restaurant) => ({
            id: restaurant.id,
            displayName: restaurant.displayName,
            formattedAddress: restaurant.formattedAddress,
            location: restaurant.location,
            photos: restaurant.photos
              ? restaurant.photos.map((photo) => ({
                  name: photo.name,
                  widthPx: photo.widthPx,
                  heightPx: photo.heightPx,
                }))
              : [],
            rating: restaurant.rating,
            userRatingCount: restaurant.userRatingCount,
            editorialSummary: restaurant.editorialSummary,
            primaryTypeDisplayName: restaurant.primaryTypeDisplayName,
            priceLevel: restaurant.priceLevel,
            websiteUri: restaurant.websiteUri,
            googleMapsUri: restaurant.googleMapsUri,
            currentOpeningHours: restaurant.currentOpeningHours,
            internationalPhoneNumber: restaurant.internationalPhoneNumber,
          }));

          // Store using chunked method
          await storeDataInChunks(client, baseCacheKey, essentialData);
          console.log(
            `[CACHE STORE] Stored restaurants data using chunked approach`
          );

          // Calculate pagination for response
          const startIndex = (page - 1) * limit;
          const endIndex = startIndex + limit;
          const paginatedRestaurants = essentialData.slice(
            startIndex,
            endIndex
          );

          const paginationInfo = {
            totalItems: essentialData.length,
            itemsPerPage: limit,
            currentPage: page,
            totalPages: Math.ceil(essentialData.length / limit),
            hasNextPage: page < Math.ceil(essentialData.length / limit),
            hasPrevPage: page > 1,
          };

          const responseData = {
            restaurants: paginatedRestaurants,
            pagination: paginationInfo,
          };

          // Removed expiry
          await client.set(paginatedCacheKey, JSON.stringify(responseData));

          const responseTime = Date.now() - startTime;
          console.log(
            `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Google API with chunked storage)`
          );

          return res
            .status(200)
            .json(
              new ApiResponse(
                200,
                responseData,
                "Restaurants fetched from Google API"
              )
            );
        } catch (cacheError) {
          console.error(
            `[CACHE ERROR] Failed to store data: ${cacheError.message}`
          );
        }
      }

      return res.status(200).json(
        new ApiResponse(
          200,
          {
            restaurants: [],
            pagination: { totalItems: 0, currentPage: page, totalPages: 0 },
          },
          "No restaurants found"
        )
      );
    } catch (fetchError) {
      console.error(
        `[FETCH ERROR] Failed to get data from Google API: ${fetchError.message}`
      );
      throw fetchError;
    }
  } catch (error) {
    console.error(`[ERROR] getRestaurants failed: ${error.message}`);
    if (error instanceof ApiError) {
      return res.status(error.statusCode).json(error);
    }
    return res
      .status(500)
      .json(new ApiError(500, "Internal server error", [error.message]));
  }
};

/**
 * Get amusement parks in Pakistan using Google Places API with pagination
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getAmusementParks = async (req, res) => {
  try {
    const startTime = Date.now();
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const targetUniqueCount = 200; // Target to fetch 200 unique amusement parks

    console.log(
      `[REQUEST] getAmusementParks - Getting amusement parks in Pakistan (page ${page}, limit ${limit})`
    );

    const baseCacheKey = `attractions:amusement_parks:pakistan`;
    const paginatedCacheKey = `${baseCacheKey}:page${page}:limit${limit}`;

    try {
      console.time("redis-connect");
      const client = await getRedisClient();
      console.timeEnd("redis-connect");

      // 1. Try paginated cache first (fastest)
      console.time("redis-get");
      let cachedData = await client.get(paginatedCacheKey);
      console.timeEnd("redis-get");

      if (cachedData) {
        console.time("redis-parse");
        const parsedData = JSON.parse(cachedData);
        console.timeEnd("redis-parse");

        console.log(
          `[CACHE HIT] Found paginated data in Redis: ${paginatedCacheKey}`
        );
        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Paginated Cache)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              parsedData,
              "Amusement parks fetched from paginated cache"
            )
          );
      }

      // 2. Try chunked storage (fast)
      console.log(`[CACHE] Trying chunked storage for ${baseCacheKey}...`);
      const chunkedData = await getDataFromChunks(
        client,
        baseCacheKey,
        page,
        limit
      );

      if (chunkedData) {
        console.log(`[CACHE HIT] Found chunked data in Redis`);

        const responseData = {
          amusementParks: chunkedData.items,
          pagination: chunkedData.pagination,
        };

        // Cache this paginated result - removed expiry
        try {
          await client.set(paginatedCacheKey, JSON.stringify(responseData));
          console.log(
            `[CACHE STORE] Stored paginated amusement parks data with key: ${paginatedCacheKey}`
          );
        } catch (cacheError) {
          console.error(
            `[CACHE ERROR] Failed to store paginated data: ${cacheError.message}`
          );
        }

        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Chunked Cache)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              responseData,
              "Amusement parks fetched from chunked cache"
            )
          );
      }

      // 3. Fallback to full dataset (will convert to chunks if found)
      console.log(
        `[CACHE] Trying full dataset for conversion: ${baseCacheKey}`
      );
      console.time("redis-get-full");
      const fullDataset = await client.get(baseCacheKey);
      console.timeEnd("redis-get-full");

      if (fullDataset) {
        console.log(
          `[CACHE HIT] Found full dataset in Redis. Converting to chunked storage...`
        );

        console.time("redis-parse-full");
        const allAmusementParks = JSON.parse(fullDataset);
        console.timeEnd("redis-parse-full");

        // Convert to chunked storage
        await convertToChunkedStorage(client, baseCacheKey, allAmusementParks);

        // Apply pagination for this request
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedAmusementParks = allAmusementParks.slice(
          startIndex,
          endIndex
        );

        const paginationInfo = {
          totalItems: allAmusementParks.length,
          itemsPerPage: limit,
          currentPage: page,
          totalPages: Math.ceil(allAmusementParks.length / limit),
          hasNextPage: page < Math.ceil(allAmusementParks.length / limit),
          hasPrevPage: page > 1,
        };

        const responseData = {
          amusementParks: paginatedAmusementParks,
          pagination: paginationInfo,
        };

        // Removed expiry
        await client.set(paginatedCacheKey, JSON.stringify(responseData));

        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Full Dataset + Conversion)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              responseData,
              "Amusement parks fetched from cache and optimized for future requests"
            )
          );
      }
    } catch (redisError) {
      console.error(
        `[REDIS ERROR] Failed to get data from cache: ${redisError.message}`
      );
    }

    // Google Places API fetch logic
    console.log(
      `[CACHE MISS] No data in Redis. Fetching from Google Places API...`
    );

    const apiKey = process.env.GOOGLE_PLACES_API_KEY;
    if (!apiKey) {
      throw new ApiError(500, "Google Places API key is not configured");
    }

    const url = "https://places.googleapis.com/v1/places:searchText";

    // Multiple queries for varied amusement park coverage
    const amusementParkQueries = [
      "theme parks in Pakistan",
      "water parks in Pakistan",
      "amusement parks in Karachi",
      "amusement parks in Lahore",
      "family entertainment centers in Pakistan",
      "adventure parks in Pakistan",
      "fun activities in Pakistan",
      "recreational parks in Pakistan",
      "children's parks in Pakistan",
      "entertainment venues in Pakistan",
      "amusement parks in Islamabad",
      "fun land Pakistan",
      "joyland Pakistan",
      "sozo water park Pakistan",
      "amusement rides Pakistan",
    ];

    const fieldMask =
      "places.displayName,places.formattedAddress,places.location,places.rating,places.userRatingCount,places.photos.name,places.photos.widthPx,places.photos.heightPx,places.editorialSummary,places.primaryTypeDisplayName,places.id,places.types,places.googleMapsUri,places.websiteUri,places.priceLevel,places.currentOpeningHours,places.internationalPhoneNumber,nextPageToken";

    try {
      // Fetch from multiple queries
      const allAmusementParks = await fetchMultipleQueries(
        amusementParkQueries,
        apiKey,
        url,
        fieldMask,
        targetUniqueCount,
        "Amusement Parks"
      );

      console.log(
        `[API SUCCESS] Total unique amusement parks fetched: ${allAmusementParks.length}`
      );

      if (allAmusementParks.length > 0) {
        try {
          const client = await getRedisClient();

          const essentialData = allAmusementParks.map((park) => ({
            id: park.id,
            displayName: park.displayName,
            formattedAddress: park.formattedAddress,
            location: park.location,
            photos: park.photos
              ? park.photos.map((photo) => ({
                  name: photo.name,
                  widthPx: photo.widthPx,
                  heightPx: photo.heightPx,
                }))
              : [],
            rating: park.rating,
            userRatingCount: park.userRatingCount,
            editorialSummary: park.editorialSummary,
            primaryTypeDisplayName: park.primaryTypeDisplayName,
            priceLevel: park.priceLevel,
            websiteUri: park.websiteUri,
            googleMapsUri: park.googleMapsUri,
            currentOpeningHours: park.currentOpeningHours,
            internationalPhoneNumber: park.internationalPhoneNumber,
          }));

          // Store using chunked method
          await storeDataInChunks(client, baseCacheKey, essentialData);
          console.log(
            `[CACHE STORE] Stored amusement parks data using chunked approach`
          );

          // Calculate pagination for response
          const startIndex = (page - 1) * limit;
          const endIndex = startIndex + limit;
          const paginatedAmusementParks = essentialData.slice(
            startIndex,
            endIndex
          );

          const paginationInfo = {
            totalItems: essentialData.length,
            itemsPerPage: limit,
            currentPage: page,
            totalPages: Math.ceil(essentialData.length / limit),
            hasNextPage: page < Math.ceil(essentialData.length / limit),
            hasPrevPage: page > 1,
          };

          const responseData = {
            amusementParks: paginatedAmusementParks,
            pagination: paginationInfo,
          };

          // Removed expiry
          await client.set(paginatedCacheKey, JSON.stringify(responseData));

          const responseTime = Date.now() - startTime;
          console.log(
            `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Google API with chunked storage)`
          );

          return res
            .status(200)
            .json(
              new ApiResponse(
                200,
                responseData,
                "Amusement parks fetched from Google API"
              )
            );
        } catch (cacheError) {
          console.error(
            `[CACHE ERROR] Failed to store data: ${cacheError.message}`
          );
        }
      }

      return res.status(200).json(
        new ApiResponse(
          200,
          {
            amusementParks: [],
            pagination: { totalItems: 0, currentPage: page, totalPages: 0 },
          },
          "No amusement parks found"
        )
      );
    } catch (fetchError) {
      console.error(
        `[FETCH ERROR] Failed to get data from Google API: ${fetchError.message}`
      );
      throw fetchError;
    }
  } catch (error) {
    console.error(`[ERROR] getAmusementParks failed: ${error.message}`);
    if (error instanceof ApiError) {
      return res.status(error.statusCode).json(error);
    }
    return res
      .status(500)
      .json(new ApiError(500, "Internal server error", [error.message]));
  }
};

/**
 * Clear the attractions cache (utility endpoint)
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const clearAttractionsCache = async (req, res) => {
  try {
    const { type } = req.params;
    let keys = [];

    const client = await getRedisClient();

    if (type === "all") {
      keys = await client.keys("attractions:*");
    } else if (type === "hotels") {
      keys = await client.keys("attractions:hotels:*");
    } else if (type === "restaurants") {
      keys = await client.keys("attractions:restaurants:*");
    } else if (type === "amusement-parks") {
      keys = await client.keys("attractions:amusement_parks:*");
    } else {
      return res
        .status(400)
        .json(
          new ApiError(
            400,
            "Invalid cache type. Use 'all', 'hotels', 'restaurants', or 'amusement-parks'"
          )
        );
    }

    if (keys.length > 0) {
      // Use pipeline for better performance
      const pipeline = client.multi();
      keys.forEach((key) => {
        pipeline.del(key);
      });
      await pipeline.exec();

      console.log(
        `[CACHE CLEAR] Cleared ${keys.length} cache keys for ${type}`
      );

      return res
        .status(200)
        .json(
          new ApiResponse(
            200,
            { clearedKeys: keys.length },
            `Cleared ${type} attractions cache`
          )
        );
    } else {
      return res
        .status(200)
        .json(
          new ApiResponse(
            200,
            { clearedKeys: 0 },
            `No cache keys found for ${type}`
          )
        );
    }
  } catch (error) {
    console.error(`[ERROR] clearAttractionsCache failed: ${error.message}`);
    return res
      .status(500)
      .json(new ApiError(500, "Failed to clear cache", [error.message]));
  }
};

/**
 * Get Redis performance info (utility endpoint)
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getRedisInfo = async (req, res) => {
  try {
    const client = await getRedisClient();
    const keys = await client.keys("attractions:*");

    // Group keys by type for better visibility
    const keyGroups = {
      metadata: keys.filter((k) => k.includes(":meta")).length,
      chunks: keys.filter((k) => k.includes(":chunk:")).length,
      paginated: keys.filter((k) => k.includes(":page")).length,
      full: keys.filter(
        (k) =>
          !k.includes(":meta") && !k.includes(":chunk:") && !k.includes(":page")
      ).length,
    };

    const memory = await client.info("memory");

    return res.status(200).json(
      new ApiResponse(
        200,
        {
          totalKeys: keys.length,
          keyBreakdown: keyGroups,
          sampleKeys: keys.slice(0, 10),
          memoryInfo: memory,
          serverTime: new Date().toISOString(),
        },
        "Redis info retrieved successfully"
      )
    );
  } catch (error) {
    console.error(`[ERROR] getRedisInfo failed: ${error.message}`);
    return res
      .status(500)
      .json(new ApiError(500, "Failed to get Redis info", [error.message]));
  }
};
