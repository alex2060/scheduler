FROM php:8.2-apache

# Enable directory listing in Apache by modifying the main config
RUN sed -i 's/Options -Indexes/Options +Indexes/' /etc/apache2/conf-enabled/docker-php.conf

# Enable mod_autoindex for better directory listing
RUN a2enmod autoindex

# Copy all files from build context
COPY . /var/www/html/

# Set proper permissions
RUN chown -R www-data:www-data /var/www/html

EXPOSE 80
