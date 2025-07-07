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

const getAllDataFromChunks = async (client, baseKey) => {
  try {
    // Get metadata
    const metaStr = await client.get(`${baseKey}:meta`);
    if (!metaStr) return null;

    const meta = JSON.parse(metaStr);
    const { chunks } = meta;

    // Fetch all chunks
    const chunkPromises = [];
    for (let i = 0; i < chunks; i++) {
      chunkPromises.push(client.get(`${baseKey}:chunk:${i}`));
    }

    const chunkResults = await Promise.all(chunkPromises);

    // Combine all chunks
    let allData = [];
    chunkResults.forEach((chunk) => {
      if (chunk) {
        allData = allData.concat(JSON.parse(chunk));
      }
    });

    return allData;
  } catch (error) {
    console.error(`[GET ALL CHUNKS ERROR] ${error.message}`);
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

// Enhanced city extraction function with smart pattern recognition
const extractCitiesFromAttractions = (attractions) => {
  const cities = new Set();

  // Known Pakistani cities list (comprehensive)
  const knownCities = [
    "Karachi",
    "Lahore",
    "Islamabad",
    "Rawalpindi",
    "Faisalabad",
    "Multan",
    "Peshawar",
    "Quetta",
    "Gujranwala",
    "Sialkot",
    "Hyderabad",
    "Bahawalpur",
    "Sargodha",
    "Sukkur",
    "Larkana",
    "Mardan",
    "Swat",
    "Abbottabad",
    "Murree",
    "Gilgit",
    "Hunza",
    "Chitral",
    "Muzaffarabad",
    "Gwadar",
    "Naran",
    "Kaghan",
    "Ziarat",
    "Mansehra",
    "Swabi",
    "Mingora",
    "Kohat",
    "Bannu",
    "Dera Ghazi Khan",
    "Sahiwal",
    "Okara",
    "Kasur",
    "Mandi Bahauddin",
    "Jhelum",
    "Chiniot",
    "Hafizabad",
    "Narowal",
    "Sheikhupura",
    "Gujrat",
    "Wah",
    "Taxila",
    "Mirpur",
    "Kotli",
    "Rawalakot",
    "Bhimber",
    "Neelum",
  ];

  // Create a case-insensitive lookup map
  const cityLookup = new Map();
  knownCities.forEach((city) => {
    cityLookup.set(city.toLowerCase(), city);
  });

  attractions.forEach((attraction) => {
    if (!attraction.formattedAddress) return;

    const address = attraction.formattedAddress;
    console.log(`[CITY EXTRACTION] Processing: ${address}`);

    // Method 1: Direct city name matching from known list
    let foundCity = false;
    for (const [lowerCity, properCity] of cityLookup.entries()) {
      // Check with word boundaries for exact match
      const regex = new RegExp(`\\b${lowerCity}\\b`, "i");
      if (regex.test(address)) {
        cities.add(properCity);
        console.log(`[CITY FOUND] Direct match: ${properCity}`);
        foundCity = true;
        break;
      }
    }

    // Method 2: Position-based extraction for addresses without direct matches
    if (!foundCity) {
      const parts = address.split(",").map((part) => part.trim());

      // Find the position of Pakistan in the address
      const pakistanIndex = parts.findIndex((part) =>
        part.toLowerCase().includes("pakistan")
      );

      if (pakistanIndex > 0) {
        // Typically city is before province, which is before country
        let cityIndex = pakistanIndex - 2; // Default position

        // Check if the part before Pakistan is a province
        const provinceKeywords = [
          "punjab",
          "sindh",
          "balochistan",
          "khyber",
          "kpk",
          "azad",
          "gilgit",
        ];
        const partBeforePakistan = parts[pakistanIndex - 1].toLowerCase();

        const isProvince = provinceKeywords.some((keyword) =>
          partBeforePakistan.includes(keyword)
        );

        if (isProvince) {
          // If we identified a province, city is likely before it
          cityIndex = pakistanIndex - 2;
        } else {
          // Otherwise, the part before Pakistan might be the city
          cityIndex = pakistanIndex - 1;
        }

        if (cityIndex >= 0 && parts[cityIndex]) {
          const potentialCity = parts[cityIndex].trim();

          // Avoid adding provinces, countries, or very short strings as cities
          if (
            potentialCity.length > 2 &&
            !provinceKeywords.some((kw) =>
              potentialCity.toLowerCase().includes(kw)
            ) &&
            !potentialCity.toLowerCase().includes("pakistan") &&
            !potentialCity.toLowerCase().includes("district")
          ) {
            // Format with proper capitalization
            const formattedCity = potentialCity
              .split(" ")
              .map(
                (word) =>
                  word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()
              )
              .join(" ");

            cities.add(formattedCity);
            console.log(`[CITY EXTRACTED] Position-based: ${formattedCity}`);
          }
        }
      }
    }
  });

  const citiesArray = Array.from(cities).sort();
  console.log(
    `[CITY EXTRACTION COMPLETE] Found ${citiesArray.length} cities: ${citiesArray.join(", ")}`
  );
  return citiesArray;
};

// Enhanced city filtering with better matching
const filterAttractionsByCity = (attractions, city) => {
  if (!city || city === "all") {
    return attractions;
  }

  console.log(
    `[CITY FILTER] Filtering ${attractions.length} attractions for city: ${city}`
  );

  // Convert to lowercase once for efficiency
  const cityLower = city.toLowerCase();

  const filtered = attractions.filter((attraction) => {
    if (!attraction.formattedAddress) return false;

    const address = attraction.formattedAddress.toLowerCase();

    // Try exact word match first (most accurate)
    const cityRegex = new RegExp(`\\b${cityLower}\\b`, "i");
    if (cityRegex.test(address)) return true;

    // Check for city followed by comma (common address pattern)
    if (address.includes(`${cityLower},`)) return true;

    // Check for city at the end of address parts
    const addressParts = address.split(",").map((part) => part.trim());
    if (addressParts.some((part) => part === cityLower)) return true;

    // Fallback to general inclusion (less precise but catches edge cases)
    return address.includes(cityLower);
  });

  console.log(
    `[CITY FILTER COMPLETE] Found ${filtered.length} attractions in ${city}`
  );
  return filtered;
};

/**
 * Get hotels in Pakistan using Google Places API with pagination and city filtering
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getHotels = async (req, res) => {
  try {
    const startTime = Date.now();
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const city = req.query.city || "all";
    const targetUniqueCount = 200;

    console.log(
      `[REQUEST] getHotels - page ${page}, limit ${limit}, city: ${city}`
    );

    const baseCacheKey = `attractions:hotels:pakistan`;
    const cityFilteredCacheKey =
      city !== "all"
        ? `${baseCacheKey}:city:${encodeURIComponent(city)}`
        : baseCacheKey;
    const paginatedCacheKey = `${cityFilteredCacheKey}:page${page}:limit${limit}`;

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

      // 2. Try getting all data from chunks and apply city filter
      console.log(
        `[CACHE] Getting all data from chunks for ${baseCacheKey}...`
      );
      const allHotelsData = await getAllDataFromChunks(client, baseCacheKey);

      if (allHotelsData && allHotelsData.length > 0) {
        console.log(
          `[CACHE HIT] Found ${allHotelsData.length} hotels in chunked data`
        );

        // Extract available cities for the frontend
        const availableCities = extractCitiesFromAttractions(allHotelsData);
        console.log(
          `[CITIES] Extracted ${availableCities.length} cities: ${availableCities.join(", ")}`
        );

        // Apply city filter
        const cityFilteredHotels = filterAttractionsByCity(allHotelsData, city);
        console.log(
          `[CITY FILTER] Filtered from ${allHotelsData.length} to ${cityFilteredHotels.length} hotels for city: ${city}`
        );

        // Apply pagination to filtered results
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedHotels = cityFilteredHotels.slice(startIndex, endIndex);

        const paginationInfo = {
          totalItems: cityFilteredHotels.length,
          itemsPerPage: limit,
          currentPage: page,
          totalPages: Math.ceil(cityFilteredHotels.length / limit),
          hasNextPage: page < Math.ceil(cityFilteredHotels.length / limit),
          hasPrevPage: page > 1,
        };

        const responseData = {
          hotels: paginatedHotels,
          pagination: paginationInfo,
          availableCities: availableCities,
          selectedCity: city,
        };

        // Cache this paginated result for faster access next time
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
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Chunked Cache with City Filter)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              responseData,
              `Hotels fetched from chunked cache${city !== "all" ? ` for ${city}` : ""}`
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

        // Extract available cities
        const availableCities = extractCitiesFromAttractions(allHotels);

        // Apply city filter
        const cityFilteredHotels = filterAttractionsByCity(allHotels, city);

        // Apply pagination to filtered results
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedHotels = cityFilteredHotels.slice(startIndex, endIndex);

        const paginationInfo = {
          totalItems: cityFilteredHotels.length,
          itemsPerPage: limit,
          currentPage: page,
          totalPages: Math.ceil(cityFilteredHotels.length / limit),
          hasNextPage: page < Math.ceil(cityFilteredHotels.length / limit),
          hasPrevPage: page > 1,
        };

        const responseData = {
          hotels: paginatedHotels,
          pagination: paginationInfo,
          availableCities: availableCities,
          selectedCity: city,
        };

        // Store paginated result in cache
        await client.set(paginatedCacheKey, JSON.stringify(responseData));

        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Full Dataset + Conversion + City Filter)`
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

          // Extract available cities
          const availableCities = extractCitiesFromAttractions(essentialData);

          // Apply city filter to fresh data
          const cityFilteredHotels = filterAttractionsByCity(
            essentialData,
            city
          );
          console.log(
            `[CITY FILTER] Filtered from ${essentialData.length} to ${cityFilteredHotels.length} hotels for city: ${city}`
          );

          // Calculate pagination for response
          const startIndex = (page - 1) * limit;
          const endIndex = startIndex + limit;
          const paginatedHotels = cityFilteredHotels.slice(
            startIndex,
            endIndex
          );

          const paginationInfo = {
            totalItems: cityFilteredHotels.length,
            itemsPerPage: limit,
            currentPage: page,
            totalPages: Math.ceil(cityFilteredHotels.length / limit),
            hasNextPage: page < Math.ceil(cityFilteredHotels.length / limit),
            hasPrevPage: page > 1,
          };

          const responseData = {
            hotels: paginatedHotels,
            pagination: paginationInfo,
            availableCities: availableCities,
            selectedCity: city,
          };

          // Store paginated data
          await client.set(paginatedCacheKey, JSON.stringify(responseData));

          const responseTime = Date.now() - startTime;
          console.log(
            `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Google API with city filter)`
          );

          return res
            .status(200)
            .json(
              new ApiResponse(
                200,
                responseData,
                "Hotels fetched from Google API with city filter"
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
            availableCities: [],
            selectedCity: city,
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
 * Get restaurants in Pakistan using Google Places API with pagination and city filtering
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getRestaurants = async (req, res) => {
  try {
    const startTime = Date.now();
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const city = req.query.city || "all";
    const targetUniqueCount = 200;

    console.log(
      `[REQUEST] getRestaurants - page ${page}, limit ${limit}, city: ${city}`
    );

    const baseCacheKey = `attractions:restaurants:pakistan`;
    const cityFilteredCacheKey =
      city !== "all"
        ? `${baseCacheKey}:city:${encodeURIComponent(city)}`
        : baseCacheKey;
    const paginatedCacheKey = `${cityFilteredCacheKey}:page${page}:limit${limit}`;

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

      // 2. Try getting all data from chunks and apply city filter
      console.log(
        `[CACHE] Getting all data from chunks for ${baseCacheKey}...`
      );
      const allRestaurantsData = await getAllDataFromChunks(
        client,
        baseCacheKey
      );

      if (allRestaurantsData && allRestaurantsData.length > 0) {
        console.log(
          `[CACHE HIT] Found ${allRestaurantsData.length} restaurants in chunked data`
        );

        // Extract available cities for the frontend
        const availableCities =
          extractCitiesFromAttractions(allRestaurantsData);

        // Apply city filter
        const cityFilteredRestaurants = filterAttractionsByCity(
          allRestaurantsData,
          city
        );
        console.log(
          `[CITY FILTER] Filtered from ${allRestaurantsData.length} to ${cityFilteredRestaurants.length} restaurants for city: ${city}`
        );

        // Apply pagination to filtered results
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedRestaurants = cityFilteredRestaurants.slice(
          startIndex,
          endIndex
        );

        const paginationInfo = {
          totalItems: cityFilteredRestaurants.length,
          itemsPerPage: limit,
          currentPage: page,
          totalPages: Math.ceil(cityFilteredRestaurants.length / limit),
          hasNextPage: page < Math.ceil(cityFilteredRestaurants.length / limit),
          hasPrevPage: page > 1,
        };

        const responseData = {
          restaurants: paginatedRestaurants,
          pagination: paginationInfo,
          availableCities: availableCities,
          selectedCity: city,
        };

        // Cache this paginated result
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
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Chunked Cache with City Filter)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              responseData,
              `Restaurants fetched from chunked cache${city !== "all" ? ` for ${city}` : ""}`
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

        // Extract available cities
        const availableCities = extractCitiesFromAttractions(allRestaurants);

        // Apply city filter
        const cityFilteredRestaurants = filterAttractionsByCity(
          allRestaurants,
          city
        );

        // Apply pagination for this request
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedRestaurants = cityFilteredRestaurants.slice(
          startIndex,
          endIndex
        );

        const paginationInfo = {
          totalItems: cityFilteredRestaurants.length,
          itemsPerPage: limit,
          currentPage: page,
          totalPages: Math.ceil(cityFilteredRestaurants.length / limit),
          hasNextPage: page < Math.ceil(cityFilteredRestaurants.length / limit),
          hasPrevPage: page > 1,
        };

        const responseData = {
          restaurants: paginatedRestaurants,
          pagination: paginationInfo,
          availableCities: availableCities,
          selectedCity: city,
        };

        await client.set(paginatedCacheKey, JSON.stringify(responseData));

        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Full Dataset + Conversion + City Filter)`
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

          // Extract available cities
          const availableCities = extractCitiesFromAttractions(essentialData);

          // Apply city filter to fresh data
          const cityFilteredRestaurants = filterAttractionsByCity(
            essentialData,
            city
          );
          console.log(
            `[CITY FILTER] Filtered from ${essentialData.length} to ${cityFilteredRestaurants.length} restaurants for city: ${city}`
          );

          // Calculate pagination for response
          const startIndex = (page - 1) * limit;
          const endIndex = startIndex + limit;
          const paginatedRestaurants = cityFilteredRestaurants.slice(
            startIndex,
            endIndex
          );

          const paginationInfo = {
            totalItems: cityFilteredRestaurants.length,
            itemsPerPage: limit,
            currentPage: page,
            totalPages: Math.ceil(cityFilteredRestaurants.length / limit),
            hasNextPage:
              page < Math.ceil(cityFilteredRestaurants.length / limit),
            hasPrevPage: page > 1,
          };

          const responseData = {
            restaurants: paginatedRestaurants,
            pagination: paginationInfo,
            availableCities: availableCities,
            selectedCity: city,
          };

          await client.set(paginatedCacheKey, JSON.stringify(responseData));

          const responseTime = Date.now() - startTime;
          console.log(
            `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Google API with city filter)`
          );

          return res
            .status(200)
            .json(
              new ApiResponse(
                200,
                responseData,
                "Restaurants fetched from Google API with city filter"
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
            availableCities: [],
            selectedCity: city,
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
 * Get amusement parks in Pakistan using Google Places API with pagination and city filtering
 * @param {Object} req - Express request object
 * @param {Object} res - Express response object
 */
export const getAmusementParks = async (req, res) => {
  try {
    const startTime = Date.now();
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 20;
    const city = req.query.city || "all";
    const targetUniqueCount = 200;

    console.log(
      `[REQUEST] getAmusementParks - page ${page}, limit ${limit}, city: ${city}`
    );

    const baseCacheKey = `attractions:amusement_parks:pakistan`;
    const cityFilteredCacheKey =
      city !== "all"
        ? `${baseCacheKey}:city:${encodeURIComponent(city)}`
        : baseCacheKey;
    const paginatedCacheKey = `${cityFilteredCacheKey}:page${page}:limit${limit}`;

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

      // 2. Try getting all data from chunks and apply city filter
      console.log(
        `[CACHE] Getting all data from chunks for ${baseCacheKey}...`
      );
      const allParksData = await getAllDataFromChunks(client, baseCacheKey);

      if (allParksData && allParksData.length > 0) {
        console.log(
          `[CACHE HIT] Found ${allParksData.length} amusement parks in chunked data`
        );

        // Extract available cities for the frontend
        const availableCities = extractCitiesFromAttractions(allParksData);

        // Apply city filter
        const cityFilteredParks = filterAttractionsByCity(allParksData, city);
        console.log(
          `[CITY FILTER] Filtered from ${allParksData.length} to ${cityFilteredParks.length} amusement parks for city: ${city}`
        );

        // Apply pagination to filtered results
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedParks = cityFilteredParks.slice(startIndex, endIndex);

        const paginationInfo = {
          totalItems: cityFilteredParks.length,
          itemsPerPage: limit,
          currentPage: page,
          totalPages: Math.ceil(cityFilteredParks.length / limit),
          hasNextPage: page < Math.ceil(cityFilteredParks.length / limit),
          hasPrevPage: page > 1,
        };

        const responseData = {
          amusementParks: paginatedParks,
          pagination: paginationInfo,
          availableCities: availableCities,
          selectedCity: city,
        };

        // Cache this paginated result
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
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Chunked Cache with City Filter)`
        );

        return res
          .status(200)
          .json(
            new ApiResponse(
              200,
              responseData,
              `Amusement parks fetched from chunked cache${city !== "all" ? ` for ${city}` : ""}`
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

        // Extract available cities
        const availableCities = extractCitiesFromAttractions(allAmusementParks);

        // Apply city filter
        const cityFilteredParks = filterAttractionsByCity(
          allAmusementParks,
          city
        );

        // Apply pagination for this request
        const startIndex = (page - 1) * limit;
        const endIndex = startIndex + limit;
        const paginatedParks = cityFilteredParks.slice(startIndex, endIndex);

        const paginationInfo = {
          totalItems: cityFilteredParks.length,
          itemsPerPage: limit,
          currentPage: page,
          totalPages: Math.ceil(cityFilteredParks.length / limit),
          hasNextPage: page < Math.ceil(cityFilteredParks.length / limit),
          hasPrevPage: page > 1,
        };

        const responseData = {
          amusementParks: paginatedParks,
          pagination: paginationInfo,
          availableCities: availableCities,
          selectedCity: city,
        };

        await client.set(paginatedCacheKey, JSON.stringify(responseData));

        const responseTime = Date.now() - startTime;
        console.log(
          `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Redis Full Dataset + Conversion + City Filter)`
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

          // Extract available cities
          const availableCities = extractCitiesFromAttractions(essentialData);

          // Apply city filter to fresh data
          const cityFilteredParks = filterAttractionsByCity(
            essentialData,
            city
          );
          console.log(
            `[CITY FILTER] Filtered from ${essentialData.length} to ${cityFilteredParks.length} amusement parks for city: ${city}`
          );

          // Calculate pagination for response
          const startIndex = (page - 1) * limit;
          const endIndex = startIndex + limit;
          const paginatedParks = cityFilteredParks.slice(startIndex, endIndex);

          const paginationInfo = {
            totalItems: cityFilteredParks.length,
            itemsPerPage: limit,
            currentPage: page,
            totalPages: Math.ceil(cityFilteredParks.length / limit),
            hasNextPage: page < Math.ceil(cityFilteredParks.length / limit),
            hasPrevPage: page > 1,
          };

          const responseData = {
            amusementParks: paginatedParks,
            pagination: paginationInfo,
            availableCities: availableCities,
            selectedCity: city,
          };

          await client.set(paginatedCacheKey, JSON.stringify(responseData));

          const responseTime = Date.now() - startTime;
          console.log(
            `[PERFORMANCE] Request completed in ${responseTime}ms (Source: Google API with city filter)`
          );

          return res
            .status(200)
            .json(
              new ApiResponse(
                200,
                responseData,
                "Amusement parks fetched from Google API with city filter"
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
            availableCities: [],
            selectedCity: city,
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
      cityFiltered: keys.filter((k) => k.includes(":city:")).length,
      full: keys.filter(
        (k) =>
          !k.includes(":meta") &&
          !k.includes(":chunk:") &&
          !k.includes(":page") &&
          !k.includes(":city:")
      ).length,
    };

    const memory = await client.info("memory");

    return res.status(200).json(
      new ApiResponse(
        200,
        {
          totalKeys: keys.length,
          keyBreakdown: keyGroups,
          sampleKeys: keys.slice(0, 15),
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
