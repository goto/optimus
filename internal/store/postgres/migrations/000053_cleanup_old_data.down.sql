ALTER TABLE hook_run DROP CONSTRAINT IF EXISTS  hook_run_job_id_fkey;
ALTER TABLE hook_run ADD CONSTRAINT hook_run_job_id_fkey
FOREIGN KEY (job_run_id) REFERENCES job_run(id) ON DELETE CASCADE;

ALTER TABLE sensor_run DROP CONSTRAINT IF EXISTS  sensor_run_job_id_fkey;
ALTER TABLE sensor_run ADD CONSTRAINT sensor_run_job_id_fkey
FOREIGN KEY (job_run_id) REFERENCES job_run(id) ON DELETE CASCADE;

ALTER TABLE task_run DROP CONSTRAINT IF EXISTS task_run_job_id_fkey;
ALTER TABLE task_run ADD CONSTRAINT task_run_job_id_fkey
FOREIGN KEY (job_run_id) REFERENCES job_run(id) ON DELETE CASCADE;

CREATE TABLE public.project_old (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    name character varying(100) NOT NULL,
    config jsonb,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    deleted_at timestamp with time zone
);
ALTER TABLE ONLY public.project_old
    ADD CONSTRAINT project_name_key UNIQUE (name);
ALTER TABLE ONLY public.project_old
    ADD CONSTRAINT project_pkey PRIMARY KEY (id);


CREATE TABLE public.namespace_old (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    project_id uuid NOT NULL,
    name character varying(100) NOT NULL,
    config jsonb,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    deleted_at timestamp with time zone
);
ALTER TABLE ONLY public.namespace_old
    ADD CONSTRAINT namespace_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.namespace_old
    ADD CONSTRAINT namespace_project_id_name_key UNIQUE (project_id, name);
ALTER TABLE ONLY public.namespace_old
    ADD CONSTRAINT namespace_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.project_old(id);

CREATE TABLE public.secret_old (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    project_id uuid NOT NULL,
    name character varying(100) NOT NULL,
    value text,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    deleted_at timestamp with time zone,
    namespace_id uuid,
    type character varying(15)
);
ALTER TABLE ONLY public.secret_old
    ADD CONSTRAINT secret_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.secret_old
    ADD CONSTRAINT secret_project_id_name_key UNIQUE (project_id, name);
CREATE INDEX secret_name_idx ON public.secret_old USING btree (name);
CREATE INDEX secret_namespace_id_idx ON public.secret_old USING btree (namespace_id);
CREATE INDEX secret_project_id_idx ON public.secret_old USING btree (project_id);
CREATE INDEX secret_type_idx ON public.secret_old USING btree (type);
ALTER TABLE ONLY public.secret_old
    ADD CONSTRAINT secret_namespace_id_fkey FOREIGN KEY (namespace_id) REFERENCES public.namespace_old(id);
ALTER TABLE ONLY public.secret_old
    ADD CONSTRAINT secret_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.project_old(id);


CREATE TABLE public.resource_old (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    project_id uuid NOT NULL,
    datastore character varying(100) NOT NULL,
    version integer,
    name character varying(250) NOT NULL,
    type character varying(100) NOT NULL,
    spec bytea,
    assets jsonb,
    labels jsonb,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    deleted_at timestamp with time zone,
    namespace_id uuid NOT NULL,
    urn character varying(300)
);
ALTER TABLE ONLY public.resource_old
    ADD CONSTRAINT resource_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.resource_old
    ADD CONSTRAINT resource_project_id_datastore_name_key UNIQUE (project_id, datastore, name);
CREATE INDEX resource_name_idx ON public.resource_old USING btree (name);
CREATE INDEX resource_namespace_id_idx ON public.resource_old USING btree (namespace_id);
CREATE INDEX resource_project_id_idx ON public.resource_old USING btree (project_id);
CREATE INDEX resource_urn_idx ON public.resource_old USING btree (urn);
ALTER TABLE ONLY public.resource_old
    ADD CONSTRAINT resource_namespace_id_fkey FOREIGN KEY (namespace_id) REFERENCES public.namespace_old(id);
ALTER TABLE ONLY public.resource_old
    ADD CONSTRAINT resource_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.project_old(id);

CREATE TABLE public.backup_old (
    id uuid NOT NULL,
    resource_id uuid NOT NULL,
    spec jsonb,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);
ALTER TABLE ONLY public.backup_old
    ADD CONSTRAINT backup_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.backup_old
    ADD CONSTRAINT backup_resource_id_fkey FOREIGN KEY (resource_id) REFERENCES public.resource_old(id) ON DELETE CASCADE;

CREATE TABLE public.replay_old (
    id uuid NOT NULL,
    job_id uuid NOT NULL,
    start_date timestamp with time zone NOT NULL,
    end_date timestamp with time zone NOT NULL,
    status character varying(30) NOT NULL,
    message jsonb,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    execution_tree jsonb,
    config jsonb
);
ALTER TABLE ONLY public.replay_old
    ADD CONSTRAINT replay_pkey PRIMARY KEY (id);

CREATE TABLE public.job_old (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    project_id uuid NOT NULL,
    version integer,
    name character varying(220) NOT NULL,
    owner character varying(100),
    start_date timestamp without time zone NOT NULL,
    end_date timestamp without time zone,
    "interval" character varying(50),
    dependencies jsonb,
    task_name character varying(200),
    task_config jsonb,
    old_window_size bigint,
    old_window_offset bigint,
    window_truncate_to character varying(10),
    assets jsonb,
    hooks jsonb,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    deleted_at timestamp with time zone,
    destination character varying(300),
    description text,
    labels jsonb,
    namespace_id uuid NOT NULL,
    behavior jsonb,
    metadata jsonb,
    external_dependencies jsonb,
    window_size character varying(10),
    window_offset character varying(10)
);
ALTER TABLE ONLY public.job_old
    ADD CONSTRAINT job_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.job_old
    ADD CONSTRAINT job_project_id_name_key UNIQUE (project_id, name);
CREATE INDEX job_old_destination_idx ON public.job_old USING btree (destination);
CREATE INDEX job_old_name_idx ON public.job_old USING btree (name);
CREATE INDEX job_old_namespace_id_idx ON public.job_old USING btree (namespace_id);
CREATE INDEX job_old_project_id_idx ON public.job_old USING btree (project_id);
ALTER TABLE ONLY public.job_old
    ADD CONSTRAINT job_namespace_id_fkey FOREIGN KEY (namespace_id) REFERENCES public.namespace_old(id);
ALTER TABLE ONLY public.job_old
    ADD CONSTRAINT job_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.project_old(id);
