-- DROP SCHEMA public;

CREATE SCHEMA staging AUTHORIZATION pg_database_owner;

CREATE TABLE staging.address_status (
	status_id int4 NOT NULL,
	address_status varchar(30) NOT NULL,
	CONSTRAINT address_status_pkey PRIMARY KEY (status_id)
);

CREATE TABLE staging.customer_address (
	customer_id int4 NOT NULL,
	address_id int4 NOT NULL,
	status_id int4 NOT NULL,
	CONSTRAINT customer_address_pkey PRIMARY KEY (customer_id, address_id)
);

CREATE TABLE staging.country (
	country_id int4 NOT NULL,
	country_name varchar(200),
	CONSTRAINT country_pkey PRIMARY KEY (country_id)
);

CREATE TABLE staging.order_status (
	status_id int4 NOT NULL,
	status_value varchar(20) NOT NULL,
	CONSTRAINT order_status_pkey PRIMARY KEY (status_id)
);

CREATE TABLE staging.order_history (
	history_id serial4 NOT NULL,
	order_id int4 NOT NULL,
	status_id int4 NOT NULL,
	status_date timestamp NOT NULL,
	CONSTRAINT order_history_pkey PRIMARY KEY (history_id)
);
CREATE INDEX idx_fk_order_id ON staging.order_history USING btree (order_id);
CREATE INDEX idx_fk_status_id ON staging.order_history USING btree (status_id);

CREATE TABLE staging.shipping_method (
	method_id int4 NOT NULL, 
	method_name varchar(100) NOT NULL,
	cost numeric(6,2) NOT NULL,
	CONSTRAINT shipping_method_pkey PRIMARY KEY (method_id)
);

CREATE TABLE staging.address (
	address_id int4 NOT NULL,
	street_number varchar(10) NOT NULL,
	street_name varchar(200) NOT NULL,
	city varchar(100) NOT NULL,
	country_id int4 NOT NULL,
	CONSTRAINT address_pkey PRIMARY KEY (address_id)
);
CREATE INDEX idx_fk_country_id ON staging.address USING btree (country_id);

CREATE TABLE staging.cust_order (
	order_id serial4 NOT NULL,
	order_date timestamp NOT NULL,
	customer_id int4 NOT NULL,
	shipping_method_id int4 NOT NULL,
	dest_address_id int4 NOT NULL,
	CONSTRAINT cust_order_pkey PRIMARY KEY (order_id)
);
CREATE INDEX idx_fk_customer_id ON staging.cust_order USING btree (customer_id);
CREATE INDEX idx_fk_shipping_method_id ON staging.cust_order USING btree (shipping_method_id);
CREATE INDEX idx_fk_dest_address_id ON staging.cust_order USING btree (dest_address_id);

CREATE TABLE staging.customer (
	customer_id int4 NOT NULL,
	first_name varchar(200) NOT NULL,
	last_name varchar(200) NOT NULL,
	email varchar(350) NOT NULL,
	CONSTRAINT customer_pkey PRIMARY KEY (customer_id)
);
CREATE INDEX idx_last_name ON staging.customer USING btree (last_name);

CREATE TABLE staging.author (
	author_id int4 NOT NULL,
	author_name varchar(400) NOT NULL,
	CONSTRAINT author_pkey PRIMARY KEY (author_id)
);

CREATE TABLE staging.book_author (
	book_id int4 NOT NULL,
	author_id int4 NOT NULL,
	CONSTRAINT book_author_pkey PRIMARY KEY (book_id, author_id)
);

CREATE TABLE staging.publisher (
	publisher_id int4 NOT NULL,
	publisher_name varchar(400) NOT null,
	constraint publisher_pkey primary key (publisher_id)
);

CREATE TABLE staging.book_language (
	language_id int4 NOT NULL,
	language_code varchar(8),
	language_name varchar (50),
	CONSTRAINT book_language_pkey PRIMARY KEY (language_id)
);

CREATE TABLE staging.book (
	book_id int4 NOT NULL,
	title varchar(400) NOT NULL,
	isbn13 varchar(13) NOT NULL,
	language_id int4 NOT NULL,
	num_pages int4 NOT NULL,
	publication_date date NOT NULL,
	publisher_id int4 NOT NULL,
	CONSTRAINT book_pkey PRIMARY KEY (book_id)
);
CREATE INDEX idx_fk_language_id ON staging.book USING btree (language_id);
CREATE INDEX idx_fk_publisher_id ON staging.book USING btree (publisher_id);
CREATE INDEX idx_title ON staging.book USING btree (title);


CREATE TABLE staging.order_line (
	line_id serial4 NOT NULL,
	order_id int4 NOT NULL,
	book_id int4 NOT NULL,
	price numeric(5, 2) NOT NULL,
	CONSTRAINT order_line_pkey PRIMARY KEY (line_id)
);
CREATE INDEX idx_fk_book_id ON staging.order_line USING btree (book_id);

ALTER TABLE ONLY staging.address
    ADD CONSTRAINT address_country_id_fkey FOREIGN KEY (country_id) REFERENCES staging.country(country_id);

ALTER TABLE ONLY staging.book
    ADD CONSTRAINT book_language_id_fkey FOREIGN KEY (language_id) REFERENCES staging.book_language(language_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.book
    ADD CONSTRAINT book_publisher_id_fkey FOREIGN KEY (publisher_id) REFERENCES staging.publisher(publisher_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.cust_order
    ADD CONSTRAINT cust_order_shipping_method_id_fkey FOREIGN KEY (shipping_method_id) REFERENCES staging.shipping_method(method_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.cust_order
    ADD CONSTRAINT cust_order_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES staging.customer(customer_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.cust_order
    ADD CONSTRAINT film_dest_address_id_fkey FOREIGN KEY (dest_address_id) REFERENCES staging.address(address_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.customer_address
    ADD CONSTRAINT customer_address_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES staging.customer(customer_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.customer_address
    ADD CONSTRAINT customer_address_address_id_fkey FOREIGN KEY (address_id) REFERENCES staging.address(address_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.customer_address
    ADD CONSTRAINT customer_address_status_id_fkey FOREIGN KEY (status_id) REFERENCES staging.address_status(status_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.order_history
    ADD CONSTRAINT order_history_order_id_fkey FOREIGN KEY (order_id) REFERENCES staging.cust_order(order_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.order_history
    ADD CONSTRAINT order_history_status_id_fkey FOREIGN KEY (status_id) REFERENCES staging.order_status(status_id) ON UPDATE CASCADE ON delete RESTRICT;

ALTER TABLE ONLY staging.order_line
    ADD CONSTRAINT order_line_order_id_fkey FOREIGN KEY (order_id) REFERENCES staging.cust_order(order_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.order_line
    ADD CONSTRAINT order_line_book_id_fkey FOREIGN KEY (book_id) REFERENCES staging.book(book_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.book_author
    ADD CONSTRAINT book_author_book_id_fkey FOREIGN KEY (book_id) REFERENCES staging.book(book_id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE ONLY staging.book_author
    ADD CONSTRAINT book_author_author_id_key FOREIGN KEY (author_id) REFERENCES staging.author(author_id);