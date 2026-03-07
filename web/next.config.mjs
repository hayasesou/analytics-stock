/** @type {import('next').NextConfig} */
const nextConfig = {
  distDir: process.env.NEXT_DIST_DIR || ".next-local",
  experimental: {
    typedRoutes: true,
  },
};

export default nextConfig;
