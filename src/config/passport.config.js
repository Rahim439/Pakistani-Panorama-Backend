import passport from "passport";
import { Strategy as GoogleStrategy } from "passport-google-oauth20";

const setupPassport = () => {
  // Serialize user
  passport.serializeUser((user, done) => {
    done(null, user);
  });

  // Deserialize user
  passport.deserializeUser((user, done) => {
    done(null, user);
  });

  // Configure Google Strategy
  passport.use(
    new GoogleStrategy(
      {
        clientID: process.env.GOOGLE_CLIENT_ID,
        clientSecret: process.env.GOOGLE_CLIENT_SECRET,
        callbackURL: `${process.env.BACKEND_URL}/api/v1/auth/google/callback`,
        scope: ["profile", "email"],
      },
      (accessToken, refreshToken, profile, done) => {
        try {
          // Extract user info from profile
          const userInfo = {
            googleId: profile.id,
            name: profile.displayName,
            email: profile.emails && profile.emails.length > 0 ? profile.emails[0].value : '',
            picture: profile.photos && profile.photos.length > 0 ? profile.photos[0].value : null,
          };
          
          done(null, userInfo);
        } catch (error) {
          done(error, null);
        }
      }
    )
  );

  return passport;
};

export default setupPassport; 