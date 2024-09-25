-- migrate up
CREATE TABLE IF NOT EXISTS location (
  id UUID      PRIMARY KEY DEFAULT uuid_generate_v4(),
  project_name VARCHAR(100) NOT NULL REFERENCES project (name),
  name         VARCHAR(100) NOT NULL,
  project      VARCHAR(100),
  dataset      VARCHAR(100),
  created_at   TIMESTAMP WITH TIME ZONE NOT NULL,
  updated_at   TIMESTAMP WITH TIME ZONE NOT NULL,

  UNIQUE (project_name, name)
);