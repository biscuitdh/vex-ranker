/** @type {import('next').NextConfig} */
const securityHeaders = [
  {
    key: "X-Content-Type-Options",
    value: "nosniff"
  },
  {
    key: "X-Frame-Options",
    value: "DENY"
  },
  {
    key: "Referrer-Policy",
    value: "strict-origin-when-cross-origin"
  },
  {
    key: "Permissions-Policy",
    value: "geolocation=(), microphone=(), camera=()"
  },
  {
    key: "Strict-Transport-Security",
    value: "max-age=31536000"
  }
];

const nextConfig = {
  transpilePackages: [
    "@vex-ranker/collector",
    "@vex-ranker/db",
    "@vex-ranker/ranking-engine",
    "@vex-ranker/vex-client"
  ],
  async headers() {
    return [
      {
        source: "/:path*",
        headers: securityHeaders
      }
    ];
  }
};

export default nextConfig;
