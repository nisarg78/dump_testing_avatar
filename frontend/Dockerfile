# =============================================================================
# Multi-stage Dockerfile for Angular AI Assistant Frontend
# Uses BMO Centralized Base Images from ECR
# Ref: Centralized Docker Base Images & Sidecars (Lakhwinder Singh)
# =============================================================================

# -- Set your AWS region (ca-central-1, us-east-1, etc.) ----------------------
ARG AWS_REGION=ca-central-1
ARG ECR_REGISTRY=260773135411.dkr.ecr.${AWS_REGION}.amazonaws.com

# -----------------------------------------------------------------------------
# Stage 1: Build  (BMO node22-httpd — AL2023 + Node 22)
# -----------------------------------------------------------------------------
FROM ${ECR_REGISTRY}/bmo-hc-base-image-repo:node22-httpd AS builder

# Set working directory
WORKDIR /app

# Install Angular CLI globally
RUN npm install -g @angular/cli@17

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --legacy-peer-deps

# Copy source code
COPY . .

# Build for production
RUN npm run build:prod

# -----------------------------------------------------------------------------
# Stage 2: Production  (BMO nginx-latest — Debian-based)
# -----------------------------------------------------------------------------
FROM ${ECR_REGISTRY}/bmo-hc-base-image-repo:nginx AS production

# Install necessary tools (Debian-based — use apt-get)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl tzdata && \
    rm -rf /var/lib/apt/lists/*

# Set timezone
ENV TZ=UTC

# Remove default nginx config
RUN rm -rf /etc/nginx/conf.d/*

# Copy custom nginx config
COPY nginx/nginx.conf /etc/nginx/nginx.conf

# Copy built application from builder stage
COPY --from=builder /app/dist/ai-assistant-frontend/browser /usr/share/nginx/html

# Create non-root user for security (Debian-based — use groupadd/useradd)
RUN groupadd -g 1001 nginx-user && \
    useradd -r -u 1001 -g nginx-user -d /var/cache/nginx -s /sbin/nologin nginx-user && \
    chown -R nginx-user:nginx-user /var/cache/nginx && \
    chown -R nginx-user:nginx-user /var/log/nginx && \
    chown -R nginx-user:nginx-user /etc/nginx/conf.d && \
    touch /var/run/nginx.pid && \
    chown -R nginx-user:nginx-user /var/run/nginx.pid && \
    chown -R nginx-user:nginx-user /usr/share/nginx/html

# Set permissions
RUN chmod -R 755 /usr/share/nginx/html

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost/health || exit 1

# Expose port
EXPOSE 80

# Run as non-root user
USER nginx-user

# Start nginx
CMD ["nginx", "-g", "daemon off;"]

# -----------------------------------------------------------------------------
# Stage: Development (optional)
# -----------------------------------------------------------------------------
FROM ${ECR_REGISTRY}/bmo-hc-base-image-repo:node20-httpd AS development

WORKDIR /app

# Install Angular CLI
RUN npm install -g @angular/cli@17

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --legacy-peer-deps

# Copy source
COPY . .

# Expose port for dev server
EXPOSE 4200

# Start development server
CMD ["npm", "start", "--", "--host", "0.0.0.0", "--poll", "2000"]
