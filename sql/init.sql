create table if not exists videos (
    id uuid primary key,
    creator_id uuid,
    video_created_at timestamp with time zone,
    views_count int,
    likes_count int,
    comments_count int,
    reports_count int,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);

create table if not exists video_snapshots (
    id uuid primary key,
    video_id uuid references videos(id) on delete cascade,
    views_count int,
    likes_count int,
    comments_count int,
    reports_count int,
    delta_views_count int,
    delta_likes_count int,
    delta_comments_count int,
    delta_reports_count int,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);

create index if not exists idx_videos_created on videos(video_created_at);
create index if not exists idx_snapshots_created on video_snapshots(created_at);
create index if not exists idx_snapshots_video_id on video_snapshots(video_id);