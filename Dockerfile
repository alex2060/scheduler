# Use official PHP Apache image
FROM php:8.2-apache

# Install additional PHP extensions if needed
RUN docker-php-ext-install pdo pdo_mysql

# Enable Apache mod_rewrite
RUN a2enmod rewrite

# Set working directory
WORKDIR /var/www/html

# Copy PHP configuration if you have one
# COPY php.ini /usr/local/etc/php/

# Set proper permissions
RUN chown -R www-data:www-data /var/www/html
RUN chmod -R 755 /var/www/html

# Change Apache port to 10000 to match docker-compose
RUN sed -i 's/80/10000/g' /etc/apache2/sites-available/000-default.conf /etc/apache2/ports.conf

# Expose port 10000
EXPOSE 10000

# Start Apache in foreground
CMD ["apache2-foreground"]