import asyncio
import base64
import gzip
import json
import os
import zipfile

from datetime import datetime, timedelta
from typing import Dict, List
from config import (
    AMPLITUDE_API_KEY,
    AMPLITUDE_SECRET_KEY,
    AMPLITUDE_PROJECT_ID,
    EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
)
from models.export_model import ExportInfo
from models.metrics_model import MetricsData, ReportMetrics
from slack_sdk.models.blocks import SectionBlock, HeaderBlock, DividerBlock
from services.auth0.auth0_service import get_auth_token, search_auth0_users
from models.project_model import ProjectInfo
from models.user_activity_model import UserActivity, UserActivityStore
from services.slack.send_message import send_slack_in_thread_message, send_slack_message
from utils import get_start_end_of_week_by_offset
from services.slack.message_templates import build_slack_template
from utils import (
    format_bytes,
    free_up_disk_space,
    format_percentage_change,
    seconds_to_readable_time,
)
from enums import AmplitudeEvents, AmplitudeQueryMetrics, MetricCategory, MetricsName
from services.amplitude.amplitude_service import (
    export_amplitude_events,
    sum_filtered_amplitude_events,
)

SLACK_MESSAGE_LIMIT = 3000
EXPORT_FILENAME = "export_data"
# Save in /tmp to make it writable in AWS Lambda.
# https://repost.aws/questions/QUyYQzTTPnRY6_2w71qscojA/read-only-file-system-aws-lambda
EXPORT_FILEPATH = f"/tmp/{EXPORT_FILENAME}"
EXTRACTED_DATA_FILEPATH = f"/tmp/{EXPORT_FILENAME}/{AMPLITUDE_PROJECT_ID}/"


def build_analytics_message(
    metrics_data: ReportMetrics, prev_period_metrics_data: ReportMetrics
) -> str:
    str_msg = ""
    for metric_type, metric_data in metrics_data.data.items():
        str_msg += f"\n>*{metric_type.value}:*"
        for metric_name, value in metric_data.metrics.items():
            whitespace = "          "

            if not value and value != 0:
                str_msg += f"\n>f{whitespace}- *{metric_name}:* No data found"
                continue

            prev_period_value = None
            prev_period_data: MetricsData = prev_period_metrics_data.data.get(
                metric_type, None
            )
            if prev_period_data:
                prev_period_value = prev_period_data.metrics.get(metric_name, None)
            print(
                f"[{metric_type.value}]Comparing metrics [{metric_name}]: ",
                value,
                prev_period_value,
            )

            compare_str = (
                "(No data for previous period)"
                if not prev_period_value and prev_period_value != 0
                else format_percentage_change(value, prev_period_value)
            )
            if metric_name == MetricsName.total_video_duration:
                value = seconds_to_readable_time(value)
            elif metric_name in [
                MetricsName.export_mp4_landscape_with_captions,
                MetricsName.export_mp4_landscape_without_captions,
                MetricsName.export_mp4_vertical_with_captions,
                MetricsName.export_mp4_vertical_without_captions,
            ]:
                whitespace *= 2

            str_msg += f"\n>{whitespace}- *{metric_name}:* {value} {compare_str}"
    return str_msg


def handle_daily_lambda_event():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    comparison_date = today - timedelta(days=2)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formated_date}) Eddie Daily Analytics"

    try:
        # Query data for daily report
        daily_metrics_data = get_analytics_metrics_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if not daily_metrics_data:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            # Query data for compare with daily report
            prev_period_metrics_data = get_analytics_metrics_data(
                comparison_date.strftime("%Y%m%d"), comparison_date.strftime("%Y%m%d")
            )
            print("Daily: ", daily_metrics_data)
            print("Compare with: ", prev_period_metrics_data)
            message = build_analytics_message(
                daily_metrics_data, prev_period_metrics_data
            )
    except Exception as e:
        print("Unexpected error occur in daily event --errors: ", e)
        message = f"> - *Error when preparing message for daily analytics*"
    print("Building slack message for daily eddie analytics...")
    return build_slack_template(header, message)


def handle_weekly_lambda_event():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    (
        monday_of_comparison_week,
        sunday_of_comparison_week,
    ) = get_start_end_of_week_by_offset(week_offset=2)

    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Weekly Analytics"

    try:
        weekly_metrics_data = get_analytics_metrics_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )

        if not weekly_metrics_data:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            prev_period_metrics_data = get_analytics_metrics_data(
                monday_of_comparison_week.strftime("%Y%m%d"),
                sunday_of_comparison_week.strftime("%Y%m%d"),
            )
            message = build_analytics_message(
                weekly_metrics_data, prev_period_metrics_data
            )

    except Exception as e:
        print("Unexpected error occur in weekly event --errors: ", e)
        message = f"> - *Error when preparing message for weekly analytics*"
    print("Building slack message for weekly eddie analytics...")
    return build_slack_template(header, message)


def get_analytics_metrics_data(start_date, end_date):
    # date params are in format str YYYYMMDD
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    project_creation_metrics = {}
    user_activity_metrics = {}
    share_acitivity_metrics = {}
    exports_metrics = {}

    # Project creation:
    # how many single cam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["singlecam"],
            }
        ],
    }

    singlecam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.singlecam_project_created: singlecam_project_created}
    )

    # how many multicam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["multicam"],
            }
        ],
    }
    multicam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.multicam_project_created: multicam_project_created}
    )

    # How many project created via web?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["browser"],
            }
        ],
    }

    project_created_by_web = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_web: project_created_by_web}
    )

    # How many project created via desktop?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["desktop"],
            }
        ],
    }

    project_created_by_desktop_app = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_desktop_app: project_created_by_desktop_app}
    )

    # User Activity:
    # How many new users?
    start_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    params = {
        "include_totals": "true",
        # auth0 require date in format YYYY-MM-DD
        "q": f"created_at:[{start_date_formatted} TO {end_date_formatted}]",
    }
    access_token = get_auth_token()
    response = search_auth0_users(params, access_token)
    new_user = response.get("total", 0)
    user_activity_metrics.update({MetricsName.new_user: new_user})

    # how many videos were uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_uploaded: file_were_uploaded})

    # how many hours of video uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
        "group_by": [
            {
                "type": "event",
                "value": "durationSec",
            }
        ],
    }
    total_video_duration = sum_filtered_amplitude_events(
        event_filters, start_date, end_date, AmplitudeQueryMetrics.sums, AMPLITUDE_AUTH
    )
    user_activity_metrics.update(
        {MetricsName.total_video_duration: total_video_duration}
    )

    # Unsuccessful Upload Attempts: (Unsupported language or no speech detected)
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_failed,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_upload_failed: file_were_uploaded})

    # how many returning users (users who already signed up who came back this week and created a new project)
    event_filters = {
        "event_type": AmplitudeEvents.returning_user,
    }
    returning_users = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.returning_user: returning_users})

    # Sharing Activity:
    # How many share links opened?
    event_filters = {
        "event_type": AmplitudeEvents.open_share_link,
    }
    share_links_opened = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.open_share_link: share_links_opened})

    # How many share links created?
    event_filters = {
        "event_type": AmplitudeEvents.create_share_link,
    }
    share_links_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.create_share_link: share_links_created})

    # Exports:
    # Total Export Mp4(Landscape)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions", "original"],
            },
        ],
    }

    total_export_lanscape = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_landscape: total_export_lanscape})

    # Export Mp4(Landscape) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions"],
            },
        ],
    }

    export_landscape_without_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {
            MetricsName.export_mp4_landscape_without_captions: export_landscape_without_captions
        }
    )

    # Export Mp4(Landscape) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original"],
            },
        ],
    }

    export_landscape_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_landscape_with_captions: export_landscape_with_captions}
    )

    # Total Export Mp4(Vertical)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions", "vertical"],
            },
        ],
    }

    total_export_vertical = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_vertical: total_export_vertical})

    # Export Mp4(Vertical) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions"],
            },
        ],
    }

    vertical_no_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_without_captions: vertical_no_captions}
    )

    # Export Mp4(Vertical) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical"],
            },
        ],
    }

    vertical_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_with_captions: vertical_with_captions}
    )

    # how many exports adobe premiere
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["premiere"],
            }
        ],
    }

    export_premiere = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_adobe: export_premiere})

    # how many exports davinci resolve
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["davinci"],
            }
        ],
    }

    export_davinci = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_resolve: export_davinci})

    # how many exports to fcp
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["fcpxml"],
            }
        ],
    }
    export_fcp = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_fcp: export_fcp})

    # how many exports (total)
    event_filters = {"event_type": AmplitudeEvents.user_click_export}
    total_exports = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.total_exports: total_exports})

    return ReportMetrics(
        data={
            MetricCategory.project_creation: MetricsData(
                metrics=project_creation_metrics
            ),
            MetricCategory.user_activity: MetricsData(metrics=user_activity_metrics),
            MetricCategory.share_activity: MetricsData(metrics=share_acitivity_metrics),
            MetricCategory.export: MetricsData(metrics=exports_metrics),
        }
    )


def parse_export_data(extracted_file_paths: List[str]) -> List[UserActivity]:
    # Summarize user activity
    user_store = UserActivityStore()

    for json_file in extracted_file_paths:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            for index, event in enumerate(data, start=1):
                if event.get("data_type") != "event":
                    continue

                user_id = (
                    event.get("user_id")
                    or f'Anonymous (Amplitude ID-{event.get("amplitude_id", index)})'
                )
                user_activity = user_store.get_activity(user_id)
                project = handle_export_event(event, user_activity)
                if project:
                    user_activity.add_project(project)
                    user_store.add_activity(user_activity)
    return user_store.get_all_activities()


def handle_export_event(event, user_activity: UserActivity):
    event_type = event.get("event_type")
    event_properties = event.get("event_properties")
    project_id = event_properties.get("projectId")
    project_name = event_properties.get("projectName")

    try:
        raw_files_info = event_properties.get("projectFileInfo", "[]")
        files_info = (
            json.loads(raw_files_info)
            if isinstance(raw_files_info, str)
            else raw_files_info
        )

        project_files_info = (
            ", ".join(
                [
                    f"{file.get('filename')} ({format_bytes(int(file.get('filesize', 0)))})"
                    for file in files_info
                ]
            )
            if files_info and isinstance(files_info, list)
            else None
        )
    except (json.JSONDecodeError, TypeError) as e:
        project_files_info = None
        print(f"Error parsing projectFileInfo: {e}")

    if event_type in {
        AmplitudeEvents.user_submit_prompt,
        AmplitudeEvents.user_click_prompt_suggest,
    }:
        prompt = (
            event_properties.get("prompt")
            if event_type == AmplitudeEvents.user_submit_prompt
            else event_properties.get("promptSuggest")
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_prompt(prompt)
        else:
            # Init new project with prompt
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[prompt],
                exports=[],
            )
        return project

    elif event_type == AmplitudeEvents.file_export_succeeded:
        layout = event_properties.get("layout")
        export_type = event_properties.get("exportType")
        ouput_duration_str = event_properties.get("outputDuration", None)
        readable_duration = (
            seconds_to_readable_time(float(ouput_duration_str))
            if ouput_duration_str
            else None
        )
        export_filename = event_properties.get("exportFileName")
        export_info = ExportInfo(
            export_layout=layout,
            export_type=export_type,
            export_file_name=export_filename,
            output_duration=readable_duration,
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_export(export_info)
        else:
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[],
                exports=[export_info],
            )
        return project
    else:
        return None


def handle_export_data_response(export_data) -> List[str]:
    os.makedirs(os.path.dirname(EXTRACTED_DATA_FILEPATH), exist_ok=True)
    # Write export data as zip
    with open(f"{EXPORT_FILEPATH}.zip", "wb") as f:
        f.write(export_data.content)
    # Extract data from zip archive
    with zipfile.ZipFile(f"{EXPORT_FILEPATH}.zip", "r") as zip_ref:
        zip_ref.extractall(f"/tmp/{EXPORT_FILENAME}")

    return os.listdir(EXTRACTED_DATA_FILEPATH)


def extract_export_data(files: List[str]) -> List[str]:
    ouput_file_paths = []

    # Extract export data to json
    for file in files:
        if file.endswith(".json.gz"):
            file_path = os.path.join(EXTRACTED_DATA_FILEPATH, file)

            extracted_data = []
            with gzip.open(file_path, "rt", encoding="utf-8") as gz_file:
                for line in gz_file:
                    json_obj = json.loads(line.strip())
                    extracted_data.append(json_obj)

            output_file = file.replace(".gz", "")
            output_path = os.path.join(EXTRACTED_DATA_FILEPATH, output_file)

            with open(output_path, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file, indent=4)

            ouput_file_paths.append(EXTRACTED_DATA_FILEPATH + output_file)
            print(f"Extracted: {output_file}")
    return ouput_file_paths


async def handle_send_daily_users(
    date: str, user_store: List[UserActivity]
) -> Dict[str, str]:
    # Send report header
    header = f"🗓️ ({date}) Daily User Interactions\n"

    await send_slack_in_thread_message(
        {
            "blocks": [
                HeaderBlock(text=f"{header}").to_dict(),
                SectionBlock(text=" ").to_dict(),
            ]
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )

    users_thread = {}
    # Send user email message and init thread
    for idx, user_activity in enumerate(user_store, start=1):
        message = f"{idx}) 👤User: {user_activity.email}\n"
        # chat.postMessage has special rate limiting conditions (1msg/s)
        # https://api.slack.com/methods/chat.postMessage#rate_limiting
        await asyncio.sleep(1)
        res = await send_slack_in_thread_message(
            {"text": message}, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        users_thread[user_activity.email] = res.get("ts")

    # Send report footer
    await send_slack_in_thread_message(
        {
            "blocks": [
                SectionBlock(
                    text=f":checkered_flag: *End of Report:* {header}"
                ).to_dict(),
                SectionBlock(text=" ").to_dict(),
                DividerBlock(type="divider").to_dict(),
            ],
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )
    return users_thread


async def handle_send_message_by_thread(
    users_thread: Dict[str, str], user_store: List[UserActivity]
):
    try:
        whitespace = "        "

        for idx, user_activity in enumerate(user_store, start=1):
            parsed_str = []

            for project_idx, project in enumerate(
                user_activity.projects.values(), start=1
            ):
                project_line = f":movie_camera: #{project_idx}. *Project name:* {project.project_name}\n"
                parsed_str.append(f"{project_line}")

                if project.file_info:
                    parsed_str.append(
                        f"{whitespace*2}- *File info:* {project.file_info}\n"
                    )

                if len(project.prompts) > 0:
                    parsed_str.append(f"{whitespace}:man-raising-hand: *Prompts:*\n")
                    for prompt_idx, prompt in enumerate(project.prompts, start=1):
                        parsed_str.append(f"{whitespace*2} - #{prompt_idx}: {prompt}\n")

                if len(project.exports) > 0:
                    parsed_str.append(f"{whitespace}:inbox_tray: *Exports:*\n")
                    for export_idx, export in enumerate(project.exports, start=1):
                        export_msg = (
                            f"{whitespace*2} - #{export_idx}:  Format: {export.export_layout}-{export.export_type}"
                            if export.export_layout
                            else f"{whitespace*2} - #{export_idx}:  Format: {export.export_type}"
                        )
                        if export.output_duration:
                            export_msg = (
                                f"{export_msg}, duration: {export.output_duration}"
                            )
                        parsed_str.append(f"{export_msg}\n")

            message_str = "".join(parsed_str)
            i = 0
            while i < len(message_str):
                end_index = min(i + SLACK_MESSAGE_LIMIT, len(message_str))
                if end_index < len(message_str) and "\n" in message_str[i:end_index]:
                    end_index = message_str.rfind("\n", i, end_index) + 1

                message = message_str[i:end_index]
                # chat.postMessage has special rate limiting conditions (1msg/s)
                # https://api.slack.com/methods/chat.postMessage#rate_limiting
                await asyncio.sleep(1)
                await send_slack_in_thread_message(
                    message={"text": message},
                    channel_id=EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
                    thread_ts=users_thread[user_activity.email],
                )
                i = end_index

    except Exception as e:
        print(f"Error handle_send_message_by_thread --errors: '{e}'")
        raise


async def handle_summarizes_user_interactions():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.strftime("%Y%m%d") + "T00"
    end_date = yesterday.strftime("%Y%m%d") + "T23"
    # format to readable date for send slack message
    formatted_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_date}) Daily User Interactions\n"

    export_data = export_amplitude_events(start_date, end_date)
    if not export_data:
        message = f"> - *Error when exporting events for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        return

    try:
        ouptut_file_paths = handle_export_data_response(export_data)
        extracted_file_paths = extract_export_data(ouptut_file_paths)
        print(
            "Exported data, preparing data for daily summarizes user interactions analytics..."
        )
        data = parse_export_data(extracted_file_paths)
        print("Sending slack messsage...")
        if len(data) == 0:
            message = f"> - *Export data not found*"
            slack_template = build_slack_template(header, message)
            await send_slack_in_thread_message(
                slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
            )
        else:
            users_thread = await handle_send_daily_users(formatted_date, data)
            await handle_send_message_by_thread(users_thread, data)
    except Exception as e:
        print(
            "Unexpected error occur in daily summarizes user interactions event --errors: ",
            e,
        )
        message = f"> - *Error when preparing data for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
    finally:
        print("Sent slack message for daily summarizes user interactions analytics")
        free_up_disk_space(f"{EXPORT_FILEPATH}.zip")
        free_up_disk_space(EXTRACTED_DATA_FILEPATH)


def get_prompting_time_data(start_date, end_date) -> tuple:
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    # Average time taken for first eddie response for all users (Avarage the first time time taken when a new user prompts.)
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "filters": [
            {
                "subprop_type": "nth_time_hack",
                "subprop_key": "nth_time_performed",
                "subprop_op": "is",
                "subprop_value": ["1"],
            }
        ],
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_first_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )

    # Average time taken for all eddie responses
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_all_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )
    return avg_first_res_in_ms, avg_all_res_in_ms


def build_prompting_time_message(avg_first_res_in_ms, avg_all_res_in_ms):
    white_space = "            "
    message_str = "> 📝 *Average time taken for*:\n"
    message_str += f">{white_space}- *All eddie responses*: {seconds_to_readable_time(avg_all_res_in_ms/1000)}\n"
    message_str += f">{white_space}- *First eddie response for new users*: {seconds_to_readable_time(avg_first_res_in_ms/1000)}\n"
    return message_str


def handle_daily_prompting_time_report():
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"⏰ ({formated_date}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            print(
                f"Daily report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing daily prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing daily prompting time report*"
    print("Building slack message for daily prompting time report...")
    return build_slack_template(header, message)


def handle_weekly_prompting_time_report():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"⏰ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            print(
                f"Weekly report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing weekly prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing weekly prompting time report*"
    print("Building slack message for weekly prompting time report...")
    return build_slack_template(header, message)
import asyncio
import base64
import gzip
import json
import os
import zipfile

from datetime import datetime, timedelta
from typing import Dict, List
from config import (
    AMPLITUDE_API_KEY,
    AMPLITUDE_SECRET_KEY,
    AMPLITUDE_PROJECT_ID,
    EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
)
from models.export_model import ExportInfo
from models.metrics_model import MetricsData, ReportMetrics
from slack_sdk.models.blocks import SectionBlock, HeaderBlock, DividerBlock
from services.auth0.auth0_service import get_auth_token, search_auth0_users
from models.project_model import ProjectInfo
from models.user_activity_model import UserActivity, UserActivityStore
from services.slack.send_message import send_slack_in_thread_message, send_slack_message
from utils import get_start_end_of_week_by_offset
from services.slack.message_templates import build_slack_template
from utils import (
    format_bytes,
    free_up_disk_space,
    format_percentage_change,
    seconds_to_readable_time,
)
from enums import AmplitudeEvents, AmplitudeQueryMetrics, MetricCategory, MetricsName
from services.amplitude.amplitude_service import (
    export_amplitude_events,
    sum_filtered_amplitude_events,
)

SLACK_MESSAGE_LIMIT = 3000
EXPORT_FILENAME = "export_data"
# Save in /tmp to make it writable in AWS Lambda.
# https://repost.aws/questions/QUyYQzTTPnRY6_2w71qscojA/read-only-file-system-aws-lambda
EXPORT_FILEPATH = f"/tmp/{EXPORT_FILENAME}"
EXTRACTED_DATA_FILEPATH = f"/tmp/{EXPORT_FILENAME}/{AMPLITUDE_PROJECT_ID}/"


def build_analytics_message(
    metrics_data: ReportMetrics, prev_period_metrics_data: ReportMetrics
) -> str:
    str_msg = ""
    for metric_type, metric_data in metrics_data.data.items():
        str_msg += f"\n>*{metric_type.value}:*"
        for metric_name, value in metric_data.metrics.items():
            whitespace = "          "

            if not value and value != 0:
                str_msg += f"\n>f{whitespace}- *{metric_name}:* No data found"
                continue

            prev_period_value = None
            prev_period_data: MetricsData = prev_period_metrics_data.data.get(
                metric_type, None
            )
            if prev_period_data:
                prev_period_value = prev_period_data.metrics.get(metric_name, None)
            print(
                f"[{metric_type.value}]Comparing metrics [{metric_name}]: ",
                value,
                prev_period_value,
            )

            compare_str = (
                "(No data for previous period)"
                if not prev_period_value and prev_period_value != 0
                else format_percentage_change(value, prev_period_value)
            )
            if metric_name == MetricsName.total_video_duration:
                value = seconds_to_readable_time(value)
            elif metric_name in [
                MetricsName.export_mp4_landscape_with_captions,
                MetricsName.export_mp4_landscape_without_captions,
                MetricsName.export_mp4_vertical_with_captions,
                MetricsName.export_mp4_vertical_without_captions,
            ]:
                whitespace *= 2

            str_msg += f"\n>{whitespace}- *{metric_name}:* {value} {compare_str}"
    return str_msg


def handle_daily_lambda_event():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    comparison_date = today - timedelta(days=2)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formated_date}) Eddie Daily Analytics"

    try:
        # Query data for daily report
        daily_metrics_data = get_analytics_metrics_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if not daily_metrics_data:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            # Query data for compare with daily report
            prev_period_metrics_data = get_analytics_metrics_data(
                comparison_date.strftime("%Y%m%d"), comparison_date.strftime("%Y%m%d")
            )
            print("Daily: ", daily_metrics_data)
            print("Compare with: ", prev_period_metrics_data)
            message = build_analytics_message(
                daily_metrics_data, prev_period_metrics_data
            )
    except Exception as e:
        print("Unexpected error occur in daily event --errors: ", e)
        message = f"> - *Error when preparing message for daily analytics*"
    print("Building slack message for daily eddie analytics...")
    return build_slack_template(header, message)


def handle_weekly_lambda_event():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    (
        monday_of_comparison_week,
        sunday_of_comparison_week,
    ) = get_start_end_of_week_by_offset(week_offset=2)

    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Weekly Analytics"

    try:
        weekly_metrics_data = get_analytics_metrics_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )

        if not weekly_metrics_data:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            prev_period_metrics_data = get_analytics_metrics_data(
                monday_of_comparison_week.strftime("%Y%m%d"),
                sunday_of_comparison_week.strftime("%Y%m%d"),
            )
            message = build_analytics_message(
                weekly_metrics_data, prev_period_metrics_data
            )

    except Exception as e:
        print("Unexpected error occur in weekly event --errors: ", e)
        message = f"> - *Error when preparing message for weekly analytics*"
    print("Building slack message for weekly eddie analytics...")
    return build_slack_template(header, message)


def get_analytics_metrics_data(start_date, end_date):
    # date params are in format str YYYYMMDD
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    project_creation_metrics = {}
    user_activity_metrics = {}
    share_acitivity_metrics = {}
    exports_metrics = {}

    # Project creation:
    # how many single cam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["singlecam"],
            }
        ],
    }

    singlecam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.singlecam_project_created: singlecam_project_created}
    )

    # how many multicam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["multicam"],
            }
        ],
    }
    multicam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.multicam_project_created: multicam_project_created}
    )

    # How many project created via web?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["browser"],
            }
        ],
    }

    project_created_by_web = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_web: project_created_by_web}
    )

    # How many project created via desktop?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["desktop"],
            }
        ],
    }

    project_created_by_desktop_app = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_desktop_app: project_created_by_desktop_app}
    )

    # User Activity:
    # How many new users?
    start_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    params = {
        "include_totals": "true",
        # auth0 require date in format YYYY-MM-DD
        "q": f"created_at:[{start_date_formatted} TO {end_date_formatted}]",
    }
    access_token = get_auth_token()
    response = search_auth0_users(params, access_token)
    new_user = response.get("total", 0)
    user_activity_metrics.update({MetricsName.new_user: new_user})

    # how many videos were uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_uploaded: file_were_uploaded})

    # how many hours of video uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
        "group_by": [
            {
                "type": "event",
                "value": "durationSec",
            }
        ],
    }
    total_video_duration = sum_filtered_amplitude_events(
        event_filters, start_date, end_date, AmplitudeQueryMetrics.sums, AMPLITUDE_AUTH
    )
    user_activity_metrics.update(
        {MetricsName.total_video_duration: total_video_duration}
    )

    # Unsuccessful Upload Attempts: (Unsupported language or no speech detected)
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_failed,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_upload_failed: file_were_uploaded})

    # how many returning users (users who already signed up who came back this week and created a new project)
    event_filters = {
        "event_type": AmplitudeEvents.returning_user,
    }
    returning_users = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.returning_user: returning_users})

    # Sharing Activity:
    # How many share links opened?
    event_filters = {
        "event_type": AmplitudeEvents.open_share_link,
    }
    share_links_opened = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.open_share_link: share_links_opened})

    # How many share links created?
    event_filters = {
        "event_type": AmplitudeEvents.create_share_link,
    }
    share_links_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.create_share_link: share_links_created})

    # Exports:
    # Total Export Mp4(Landscape)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions", "original"],
            },
        ],
    }

    total_export_lanscape = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_landscape: total_export_lanscape})

    # Export Mp4(Landscape) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions"],
            },
        ],
    }

    export_landscape_without_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {
            MetricsName.export_mp4_landscape_without_captions: export_landscape_without_captions
        }
    )

    # Export Mp4(Landscape) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original"],
            },
        ],
    }

    export_landscape_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_landscape_with_captions: export_landscape_with_captions}
    )

    # Total Export Mp4(Vertical)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions", "vertical"],
            },
        ],
    }

    total_export_vertical = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_vertical: total_export_vertical})

    # Export Mp4(Vertical) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions"],
            },
        ],
    }

    vertical_no_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_without_captions: vertical_no_captions}
    )

    # Export Mp4(Vertical) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical"],
            },
        ],
    }

    vertical_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_with_captions: vertical_with_captions}
    )

    # how many exports adobe premiere
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["premiere"],
            }
        ],
    }

    export_premiere = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_adobe: export_premiere})

    # how many exports davinci resolve
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["davinci"],
            }
        ],
    }

    export_davinci = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_resolve: export_davinci})

    # how many exports to fcp
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["fcpxml"],
            }
        ],
    }
    export_fcp = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_fcp: export_fcp})

    # how many exports (total)
    event_filters = {"event_type": AmplitudeEvents.user_click_export}
    total_exports = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.total_exports: total_exports})

    return ReportMetrics(
        data={
            MetricCategory.project_creation: MetricsData(
                metrics=project_creation_metrics
            ),
            MetricCategory.user_activity: MetricsData(metrics=user_activity_metrics),
            MetricCategory.share_activity: MetricsData(metrics=share_acitivity_metrics),
            MetricCategory.export: MetricsData(metrics=exports_metrics),
        }
    )


def parse_export_data(extracted_file_paths: List[str]) -> List[UserActivity]:
    # Summarize user activity
    user_store = UserActivityStore()

    for json_file in extracted_file_paths:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            for index, event in enumerate(data, start=1):
                if event.get("data_type") != "event":
                    continue

                user_id = (
                    event.get("user_id")
                    or f'Anonymous (Amplitude ID-{event.get("amplitude_id", index)})'
                )
                user_activity = user_store.get_activity(user_id)
                project = handle_export_event(event, user_activity)
                if project:
                    user_activity.add_project(project)
                    user_store.add_activity(user_activity)
    return user_store.get_all_activities()


def handle_export_event(event, user_activity: UserActivity):
    event_type = event.get("event_type")
    event_properties = event.get("event_properties")
    project_id = event_properties.get("projectId")
    project_name = event_properties.get("projectName")

    try:
        raw_files_info = event_properties.get("projectFileInfo", "[]")
        files_info = (
            json.loads(raw_files_info)
            if isinstance(raw_files_info, str)
            else raw_files_info
        )

        project_files_info = (
            ", ".join(
                [
                    f"{file.get('filename')} ({format_bytes(int(file.get('filesize', 0)))})"
                    for file in files_info
                ]
            )
            if files_info and isinstance(files_info, list)
            else None
        )
    except (json.JSONDecodeError, TypeError) as e:
        project_files_info = None
        print(f"Error parsing projectFileInfo: {e}")

    if event_type in {
        AmplitudeEvents.user_submit_prompt,
        AmplitudeEvents.user_click_prompt_suggest,
    }:
        prompt = (
            event_properties.get("prompt")
            if event_type == AmplitudeEvents.user_submit_prompt
            else event_properties.get("promptSuggest")
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_prompt(prompt)
        else:
            # Init new project with prompt
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[prompt],
                exports=[],
            )
        return project

    elif event_type == AmplitudeEvents.file_export_succeeded:
        layout = event_properties.get("layout")
        export_type = event_properties.get("exportType")
        ouput_duration_str = event_properties.get("outputDuration", None)
        readable_duration = (
            seconds_to_readable_time(float(ouput_duration_str))
            if ouput_duration_str
            else None
        )
        export_filename = event_properties.get("exportFileName")
        export_info = ExportInfo(
            export_layout=layout,
            export_type=export_type,
            export_file_name=export_filename,
            output_duration=readable_duration,
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_export(export_info)
        else:
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[],
                exports=[export_info],
            )
        return project
    else:
        return None


def handle_export_data_response(export_data) -> List[str]:
    os.makedirs(os.path.dirname(EXTRACTED_DATA_FILEPATH), exist_ok=True)
    # Write export data as zip
    with open(f"{EXPORT_FILEPATH}.zip", "wb") as f:
        f.write(export_data.content)
    # Extract data from zip archive
    with zipfile.ZipFile(f"{EXPORT_FILEPATH}.zip", "r") as zip_ref:
        zip_ref.extractall(f"/tmp/{EXPORT_FILENAME}")

    return os.listdir(EXTRACTED_DATA_FILEPATH)


def extract_export_data(files: List[str]) -> List[str]:
    ouput_file_paths = []

    # Extract export data to json
    for file in files:
        if file.endswith(".json.gz"):
            file_path = os.path.join(EXTRACTED_DATA_FILEPATH, file)

            extracted_data = []
            with gzip.open(file_path, "rt", encoding="utf-8") as gz_file:
                for line in gz_file:
                    json_obj = json.loads(line.strip())
                    extracted_data.append(json_obj)

            output_file = file.replace(".gz", "")
            output_path = os.path.join(EXTRACTED_DATA_FILEPATH, output_file)

            with open(output_path, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file, indent=4)

            ouput_file_paths.append(EXTRACTED_DATA_FILEPATH + output_file)
            print(f"Extracted: {output_file}")
    return ouput_file_paths


async def handle_send_daily_users(
    date: str, user_store: List[UserActivity]
) -> Dict[str, str]:
    # Send report header
    header = f"🗓️ ({date}) Daily User Interactions\n"

    await send_slack_in_thread_message(
        {
            "blocks": [
                HeaderBlock(text=f"{header}").to_dict(),
                SectionBlock(text=" ").to_dict(),
            ]
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )

    users_thread = {}
    # Send user email message and init thread
    for idx, user_activity in enumerate(user_store, start=1):
        message = f"{idx}) 👤User: {user_activity.email}\n"
        # chat.postMessage has special rate limiting conditions (1msg/s)
        # https://api.slack.com/methods/chat.postMessage#rate_limiting
        await asyncio.sleep(1)
        res = await send_slack_in_thread_message(
            {"text": message}, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        users_thread[user_activity.email] = res.get("ts")

    # Send report footer
    await send_slack_in_thread_message(
        {
            "blocks": [
                SectionBlock(
                    text=f":checkered_flag: *End of Report:* {header}"
                ).to_dict(),
                SectionBlock(text=" ").to_dict(),
                DividerBlock(type="divider").to_dict(),
            ],
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )
    return users_thread


async def handle_send_message_by_thread(
    users_thread: Dict[str, str], user_store: List[UserActivity]
):
    try:
        whitespace = "        "

        for idx, user_activity in enumerate(user_store, start=1):
            parsed_str = []

            for project_idx, project in enumerate(
                user_activity.projects.values(), start=1
            ):
                project_line = f":movie_camera: #{project_idx}. *Project name:* {project.project_name}\n"
                parsed_str.append(f"{project_line}")

                if project.file_info:
                    parsed_str.append(
                        f"{whitespace*2}- *File info:* {project.file_info}\n"
                    )

                if len(project.prompts) > 0:
                    parsed_str.append(f"{whitespace}:man-raising-hand: *Prompts:*\n")
                    for prompt_idx, prompt in enumerate(project.prompts, start=1):
                        parsed_str.append(f"{whitespace*2} - #{prompt_idx}: {prompt}\n")

                if len(project.exports) > 0:
                    parsed_str.append(f"{whitespace}:inbox_tray: *Exports:*\n")
                    for export_idx, export in enumerate(project.exports, start=1):
                        export_msg = (
                            f"{whitespace*2} - #{export_idx}:  Format: {export.export_layout}-{export.export_type}"
                            if export.export_layout
                            else f"{whitespace*2} - #{export_idx}:  Format: {export.export_type}"
                        )
                        if export.output_duration:
                            export_msg = (
                                f"{export_msg}, duration: {export.output_duration}"
                            )
                        parsed_str.append(f"{export_msg}\n")

            message_str = "".join(parsed_str)
            i = 0
            while i < len(message_str):
                end_index = min(i + SLACK_MESSAGE_LIMIT, len(message_str))
                if end_index < len(message_str) and "\n" in message_str[i:end_index]:
                    end_index = message_str.rfind("\n", i, end_index) + 1

                message = message_str[i:end_index]
                # chat.postMessage has special rate limiting conditions (1msg/s)
                # https://api.slack.com/methods/chat.postMessage#rate_limiting
                await asyncio.sleep(1)
                await send_slack_in_thread_message(
                    message={"text": message},
                    channel_id=EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
                    thread_ts=users_thread[user_activity.email],
                )
                i = end_index

    except Exception as e:
        print(f"Error handle_send_message_by_thread --errors: '{e}'")
        raise


async def handle_summarizes_user_interactions():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.strftime("%Y%m%d") + "T00"
    end_date = yesterday.strftime("%Y%m%d") + "T23"
    # format to readable date for send slack message
    formatted_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_date}) Daily User Interactions\n"

    export_data = export_amplitude_events(start_date, end_date)
    if not export_data:
        message = f"> - *Error when exporting events for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        return

    try:
        ouptut_file_paths = handle_export_data_response(export_data)
        extracted_file_paths = extract_export_data(ouptut_file_paths)
        print(
            "Exported data, preparing data for daily summarizes user interactions analytics..."
        )
        data = parse_export_data(extracted_file_paths)
        print("Sending slack messsage...")
        if len(data) == 0:
            message = f"> - *Export data not found*"
            slack_template = build_slack_template(header, message)
            await send_slack_in_thread_message(
                slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
            )
        else:
            users_thread = await handle_send_daily_users(formatted_date, data)
            await handle_send_message_by_thread(users_thread, data)
    except Exception as e:
        print(
            "Unexpected error occur in daily summarizes user interactions event --errors: ",
            e,
        )
        message = f"> - *Error when preparing data for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
    finally:
        print("Sent slack message for daily summarizes user interactions analytics")
        free_up_disk_space(f"{EXPORT_FILEPATH}.zip")
        free_up_disk_space(EXTRACTED_DATA_FILEPATH)


def get_prompting_time_data(start_date, end_date) -> tuple:
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    # Average time taken for first eddie response for all users (Avarage the first time time taken when a new user prompts.)
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "filters": [
            {
                "subprop_type": "nth_time_hack",
                "subprop_key": "nth_time_performed",
                "subprop_op": "is",
                "subprop_value": ["1"],
            }
        ],
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_first_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )

    # Average time taken for all eddie responses
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_all_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )
    return avg_first_res_in_ms, avg_all_res_in_ms


def build_prompting_time_message(avg_first_res_in_ms, avg_all_res_in_ms):
    white_space = "            "
    message_str = "> 📝 *Average time taken for*:\n"
    message_str += f">{white_space}- *All eddie responses*: {seconds_to_readable_time(avg_all_res_in_ms/1000)}\n"
    message_str += f">{white_space}- *First eddie response for new users*: {seconds_to_readable_time(avg_first_res_in_ms/1000)}\n"
    return message_str


def handle_daily_prompting_time_report():
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"⏰ ({formated_date}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            print(
                f"Daily report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing daily prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing daily prompting time report*"
    print("Building slack message for daily prompting time report...")
    return build_slack_template(header, message)


def handle_weekly_prompting_time_report():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"⏰ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            print(
                f"Weekly report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing weekly prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing weekly prompting time report*"
    print("Building slack message for weekly prompting time report...")
    return build_slack_template(header, message)
import asyncio
import base64
import gzip
import json
import os
import zipfile

from datetime import datetime, timedelta
from typing import Dict, List
from config import (
    AMPLITUDE_API_KEY,
    AMPLITUDE_SECRET_KEY,
    AMPLITUDE_PROJECT_ID,
    EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
)
from models.export_model import ExportInfo
from models.metrics_model import MetricsData, ReportMetrics
from slack_sdk.models.blocks import SectionBlock, HeaderBlock, DividerBlock
from services.auth0.auth0_service import get_auth_token, search_auth0_users
from models.project_model import ProjectInfo
from models.user_activity_model import UserActivity, UserActivityStore
from services.slack.send_message import send_slack_in_thread_message, send_slack_message
from utils import get_start_end_of_week_by_offset
from services.slack.message_templates import build_slack_template
from utils import (
    format_bytes,
    free_up_disk_space,
    format_percentage_change,
    seconds_to_readable_time,
)
from enums import AmplitudeEvents, AmplitudeQueryMetrics, MetricCategory, MetricsName
from services.amplitude.amplitude_service import (
    export_amplitude_events,
    sum_filtered_amplitude_events,
)

SLACK_MESSAGE_LIMIT = 3000
EXPORT_FILENAME = "export_data"
# Save in /tmp to make it writable in AWS Lambda.
# https://repost.aws/questions/QUyYQzTTPnRY6_2w71qscojA/read-only-file-system-aws-lambda
EXPORT_FILEPATH = f"/tmp/{EXPORT_FILENAME}"
EXTRACTED_DATA_FILEPATH = f"/tmp/{EXPORT_FILENAME}/{AMPLITUDE_PROJECT_ID}/"


def build_analytics_message(
    metrics_data: ReportMetrics, prev_period_metrics_data: ReportMetrics
) -> str:
    str_msg = ""
    for metric_type, metric_data in metrics_data.data.items():
        str_msg += f"\n>*{metric_type.value}:*"
        for metric_name, value in metric_data.metrics.items():
            whitespace = "          "

            if not value and value != 0:
                str_msg += f"\n>f{whitespace}- *{metric_name}:* No data found"
                continue

            prev_period_value = None
            prev_period_data: MetricsData = prev_period_metrics_data.data.get(
                metric_type, None
            )
            if prev_period_data:
                prev_period_value = prev_period_data.metrics.get(metric_name, None)
            print(
                f"[{metric_type.value}]Comparing metrics [{metric_name}]: ",
                value,
                prev_period_value,
            )

            compare_str = (
                "(No data for previous period)"
                if not prev_period_value and prev_period_value != 0
                else format_percentage_change(value, prev_period_value)
            )
            if metric_name == MetricsName.total_video_duration:
                value = seconds_to_readable_time(value)
            elif metric_name in [
                MetricsName.export_mp4_landscape_with_captions,
                MetricsName.export_mp4_landscape_without_captions,
                MetricsName.export_mp4_vertical_with_captions,
                MetricsName.export_mp4_vertical_without_captions,
            ]:
                whitespace *= 2

            str_msg += f"\n>{whitespace}- *{metric_name}:* {value} {compare_str}"
    return str_msg


def handle_daily_lambda_event():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    comparison_date = today - timedelta(days=2)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formated_date}) Eddie Daily Analytics"

    try:
        # Query data for daily report
        daily_metrics_data = get_analytics_metrics_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if not daily_metrics_data:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            # Query data for compare with daily report
            prev_period_metrics_data = get_analytics_metrics_data(
                comparison_date.strftime("%Y%m%d"), comparison_date.strftime("%Y%m%d")
            )
            print("Daily: ", daily_metrics_data)
            print("Compare with: ", prev_period_metrics_data)
            message = build_analytics_message(
                daily_metrics_data, prev_period_metrics_data
            )
    except Exception as e:
        print("Unexpected error occur in daily event --errors: ", e)
        message = f"> - *Error when preparing message for daily analytics*"
    print("Building slack message for daily eddie analytics...")
    return build_slack_template(header, message)


def handle_weekly_lambda_event():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    (
        monday_of_comparison_week,
        sunday_of_comparison_week,
    ) = get_start_end_of_week_by_offset(week_offset=2)

    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Weekly Analytics"

    try:
        weekly_metrics_data = get_analytics_metrics_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )

        if not weekly_metrics_data:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            prev_period_metrics_data = get_analytics_metrics_data(
                monday_of_comparison_week.strftime("%Y%m%d"),
                sunday_of_comparison_week.strftime("%Y%m%d"),
            )
            message = build_analytics_message(
                weekly_metrics_data, prev_period_metrics_data
            )

    except Exception as e:
        print("Unexpected error occur in weekly event --errors: ", e)
        message = f"> - *Error when preparing message for weekly analytics*"
    print("Building slack message for weekly eddie analytics...")
    return build_slack_template(header, message)


def get_analytics_metrics_data(start_date, end_date):
    # date params are in format str YYYYMMDD
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    project_creation_metrics = {}
    user_activity_metrics = {}
    share_acitivity_metrics = {}
    exports_metrics = {}

    # Project creation:
    # how many single cam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["singlecam"],
            }
        ],
    }

    singlecam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.singlecam_project_created: singlecam_project_created}
    )

    # how many multicam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["multicam"],
            }
        ],
    }
    multicam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.multicam_project_created: multicam_project_created}
    )

    # How many project created via web?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["browser"],
            }
        ],
    }

    project_created_by_web = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_web: project_created_by_web}
    )

    # How many project created via desktop?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["desktop"],
            }
        ],
    }

    project_created_by_desktop_app = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_desktop_app: project_created_by_desktop_app}
    )

    # User Activity:
    # How many new users?
    start_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    params = {
        "include_totals": "true",
        # auth0 require date in format YYYY-MM-DD
        "q": f"created_at:[{start_date_formatted} TO {end_date_formatted}]",
    }
    access_token = get_auth_token()
    response = search_auth0_users(params, access_token)
    new_user = response.get("total", 0)
    user_activity_metrics.update({MetricsName.new_user: new_user})

    # how many videos were uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_uploaded: file_were_uploaded})

    # how many hours of video uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
        "group_by": [
            {
                "type": "event",
                "value": "durationSec",
            }
        ],
    }
    total_video_duration = sum_filtered_amplitude_events(
        event_filters, start_date, end_date, AmplitudeQueryMetrics.sums, AMPLITUDE_AUTH
    )
    user_activity_metrics.update(
        {MetricsName.total_video_duration: total_video_duration}
    )

    # Unsuccessful Upload Attempts: (Unsupported language or no speech detected)
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_failed,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_upload_failed: file_were_uploaded})

    # how many returning users (users who already signed up who came back this week and created a new project)
    event_filters = {
        "event_type": AmplitudeEvents.returning_user,
    }
    returning_users = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.returning_user: returning_users})

    # Sharing Activity:
    # How many share links opened?
    event_filters = {
        "event_type": AmplitudeEvents.open_share_link,
    }
    share_links_opened = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.open_share_link: share_links_opened})

    # How many share links created?
    event_filters = {
        "event_type": AmplitudeEvents.create_share_link,
    }
    share_links_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.create_share_link: share_links_created})

    # Exports:
    # Total Export Mp4(Landscape)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions", "original"],
            },
        ],
    }

    total_export_lanscape = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_landscape: total_export_lanscape})

    # Export Mp4(Landscape) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions"],
            },
        ],
    }

    export_landscape_without_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {
            MetricsName.export_mp4_landscape_without_captions: export_landscape_without_captions
        }
    )

    # Export Mp4(Landscape) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original"],
            },
        ],
    }

    export_landscape_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_landscape_with_captions: export_landscape_with_captions}
    )

    # Total Export Mp4(Vertical)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions", "vertical"],
            },
        ],
    }

    total_export_vertical = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_vertical: total_export_vertical})

    # Export Mp4(Vertical) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions"],
            },
        ],
    }

    vertical_no_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_without_captions: vertical_no_captions}
    )

    # Export Mp4(Vertical) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical"],
            },
        ],
    }

    vertical_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_with_captions: vertical_with_captions}
    )

    # how many exports adobe premiere
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["premiere"],
            }
        ],
    }

    export_premiere = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_adobe: export_premiere})

    # how many exports davinci resolve
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["davinci"],
            }
        ],
    }

    export_davinci = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_resolve: export_davinci})

    # how many exports to fcp
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["fcpxml"],
            }
        ],
    }
    export_fcp = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_fcp: export_fcp})

    # how many exports (total)
    event_filters = {"event_type": AmplitudeEvents.user_click_export}
    total_exports = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.total_exports: total_exports})

    return ReportMetrics(
        data={
            MetricCategory.project_creation: MetricsData(
                metrics=project_creation_metrics
            ),
            MetricCategory.user_activity: MetricsData(metrics=user_activity_metrics),
            MetricCategory.share_activity: MetricsData(metrics=share_acitivity_metrics),
            MetricCategory.export: MetricsData(metrics=exports_metrics),
        }
    )


def parse_export_data(extracted_file_paths: List[str]) -> List[UserActivity]:
    # Summarize user activity
    user_store = UserActivityStore()

    for json_file in extracted_file_paths:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            for index, event in enumerate(data, start=1):
                if event.get("data_type") != "event":
                    continue

                user_id = (
                    event.get("user_id")
                    or f'Anonymous (Amplitude ID-{event.get("amplitude_id", index)})'
                )
                user_activity = user_store.get_activity(user_id)
                project = handle_export_event(event, user_activity)
                if project:
                    user_activity.add_project(project)
                    user_store.add_activity(user_activity)
    return user_store.get_all_activities()


def handle_export_event(event, user_activity: UserActivity):
    event_type = event.get("event_type")
    event_properties = event.get("event_properties")
    project_id = event_properties.get("projectId")
    project_name = event_properties.get("projectName")

    try:
        raw_files_info = event_properties.get("projectFileInfo", "[]")
        files_info = (
            json.loads(raw_files_info)
            if isinstance(raw_files_info, str)
            else raw_files_info
        )

        project_files_info = (
            ", ".join(
                [
                    f"{file.get('filename')} ({format_bytes(int(file.get('filesize', 0)))})"
                    for file in files_info
                ]
            )
            if files_info and isinstance(files_info, list)
            else None
        )
    except (json.JSONDecodeError, TypeError) as e:
        project_files_info = None
        print(f"Error parsing projectFileInfo: {e}")

    if event_type in {
        AmplitudeEvents.user_submit_prompt,
        AmplitudeEvents.user_click_prompt_suggest,
    }:
        prompt = (
            event_properties.get("prompt")
            if event_type == AmplitudeEvents.user_submit_prompt
            else event_properties.get("promptSuggest")
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_prompt(prompt)
        else:
            # Init new project with prompt
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[prompt],
                exports=[],
            )
        return project

    elif event_type == AmplitudeEvents.file_export_succeeded:
        layout = event_properties.get("layout")
        export_type = event_properties.get("exportType")
        ouput_duration_str = event_properties.get("outputDuration", None)
        readable_duration = (
            seconds_to_readable_time(float(ouput_duration_str))
            if ouput_duration_str
            else None
        )
        export_filename = event_properties.get("exportFileName")
        export_info = ExportInfo(
            export_layout=layout,
            export_type=export_type,
            export_file_name=export_filename,
            output_duration=readable_duration,
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_export(export_info)
        else:
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[],
                exports=[export_info],
            )
        return project
    else:
        return None


def handle_export_data_response(export_data) -> List[str]:
    os.makedirs(os.path.dirname(EXTRACTED_DATA_FILEPATH), exist_ok=True)
    # Write export data as zip
    with open(f"{EXPORT_FILEPATH}.zip", "wb") as f:
        f.write(export_data.content)
    # Extract data from zip archive
    with zipfile.ZipFile(f"{EXPORT_FILEPATH}.zip", "r") as zip_ref:
        zip_ref.extractall(f"/tmp/{EXPORT_FILENAME}")

    return os.listdir(EXTRACTED_DATA_FILEPATH)


def extract_export_data(files: List[str]) -> List[str]:
    ouput_file_paths = []

    # Extract export data to json
    for file in files:
        if file.endswith(".json.gz"):
            file_path = os.path.join(EXTRACTED_DATA_FILEPATH, file)

            extracted_data = []
            with gzip.open(file_path, "rt", encoding="utf-8") as gz_file:
                for line in gz_file:
                    json_obj = json.loads(line.strip())
                    extracted_data.append(json_obj)

            output_file = file.replace(".gz", "")
            output_path = os.path.join(EXTRACTED_DATA_FILEPATH, output_file)

            with open(output_path, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file, indent=4)

            ouput_file_paths.append(EXTRACTED_DATA_FILEPATH + output_file)
            print(f"Extracted: {output_file}")
    return ouput_file_paths


async def handle_send_daily_users(
    date: str, user_store: List[UserActivity]
) -> Dict[str, str]:
    # Send report header
    header = f"🗓️ ({date}) Daily User Interactions\n"

    await send_slack_in_thread_message(
        {
            "blocks": [
                HeaderBlock(text=f"{header}").to_dict(),
                SectionBlock(text=" ").to_dict(),
            ]
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )

    users_thread = {}
    # Send user email message and init thread
    for idx, user_activity in enumerate(user_store, start=1):
        message = f"{idx}) 👤User: {user_activity.email}\n"
        # chat.postMessage has special rate limiting conditions (1msg/s)
        # https://api.slack.com/methods/chat.postMessage#rate_limiting
        await asyncio.sleep(1)
        res = await send_slack_in_thread_message(
            {"text": message}, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        users_thread[user_activity.email] = res.get("ts")

    # Send report footer
    await send_slack_in_thread_message(
        {
            "blocks": [
                SectionBlock(
                    text=f":checkered_flag: *End of Report:* {header}"
                ).to_dict(),
                SectionBlock(text=" ").to_dict(),
                DividerBlock(type="divider").to_dict(),
            ],
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )
    return users_thread


async def handle_send_message_by_thread(
    users_thread: Dict[str, str], user_store: List[UserActivity]
):
    try:
        whitespace = "        "

        for idx, user_activity in enumerate(user_store, start=1):
            parsed_str = []

            for project_idx, project in enumerate(
                user_activity.projects.values(), start=1
            ):
                project_line = f":movie_camera: #{project_idx}. *Project name:* {project.project_name}\n"
                parsed_str.append(f"{project_line}")

                if project.file_info:
                    parsed_str.append(
                        f"{whitespace*2}- *File info:* {project.file_info}\n"
                    )

                if len(project.prompts) > 0:
                    parsed_str.append(f"{whitespace}:man-raising-hand: *Prompts:*\n")
                    for prompt_idx, prompt in enumerate(project.prompts, start=1):
                        parsed_str.append(f"{whitespace*2} - #{prompt_idx}: {prompt}\n")

                if len(project.exports) > 0:
                    parsed_str.append(f"{whitespace}:inbox_tray: *Exports:*\n")
                    for export_idx, export in enumerate(project.exports, start=1):
                        export_msg = (
                            f"{whitespace*2} - #{export_idx}:  Format: {export.export_layout}-{export.export_type}"
                            if export.export_layout
                            else f"{whitespace*2} - #{export_idx}:  Format: {export.export_type}"
                        )
                        if export.output_duration:
                            export_msg = (
                                f"{export_msg}, duration: {export.output_duration}"
                            )
                        parsed_str.append(f"{export_msg}\n")

            message_str = "".join(parsed_str)
            i = 0
            while i < len(message_str):
                end_index = min(i + SLACK_MESSAGE_LIMIT, len(message_str))
                if end_index < len(message_str) and "\n" in message_str[i:end_index]:
                    end_index = message_str.rfind("\n", i, end_index) + 1

                message = message_str[i:end_index]
                # chat.postMessage has special rate limiting conditions (1msg/s)
                # https://api.slack.com/methods/chat.postMessage#rate_limiting
                await asyncio.sleep(1)
                await send_slack_in_thread_message(
                    message={"text": message},
                    channel_id=EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
                    thread_ts=users_thread[user_activity.email],
                )
                i = end_index

    except Exception as e:
        print(f"Error handle_send_message_by_thread --errors: '{e}'")
        raise


async def handle_summarizes_user_interactions():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.strftime("%Y%m%d") + "T00"
    end_date = yesterday.strftime("%Y%m%d") + "T23"
    # format to readable date for send slack message
    formatted_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_date}) Daily User Interactions\n"

    export_data = export_amplitude_events(start_date, end_date)
    if not export_data:
        message = f"> - *Error when exporting events for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        return

    try:
        ouptut_file_paths = handle_export_data_response(export_data)
        extracted_file_paths = extract_export_data(ouptut_file_paths)
        print(
            "Exported data, preparing data for daily summarizes user interactions analytics..."
        )
        data = parse_export_data(extracted_file_paths)
        print("Sending slack messsage...")
        if len(data) == 0:
            message = f"> - *Export data not found*"
            slack_template = build_slack_template(header, message)
            await send_slack_in_thread_message(
                slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
            )
        else:
            users_thread = await handle_send_daily_users(formatted_date, data)
            await handle_send_message_by_thread(users_thread, data)
    except Exception as e:
        print(
            "Unexpected error occur in daily summarizes user interactions event --errors: ",
            e,
        )
        message = f"> - *Error when preparing data for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
    finally:
        print("Sent slack message for daily summarizes user interactions analytics")
        free_up_disk_space(f"{EXPORT_FILEPATH}.zip")
        free_up_disk_space(EXTRACTED_DATA_FILEPATH)


def get_prompting_time_data(start_date, end_date) -> tuple:
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    # Average time taken for first eddie response for all users (Avarage the first time time taken when a new user prompts.)
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "filters": [
            {
                "subprop_type": "nth_time_hack",
                "subprop_key": "nth_time_performed",
                "subprop_op": "is",
                "subprop_value": ["1"],
            }
        ],
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_first_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )

    # Average time taken for all eddie responses
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_all_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )
    return avg_first_res_in_ms, avg_all_res_in_ms


def build_prompting_time_message(avg_first_res_in_ms, avg_all_res_in_ms):
    white_space = "            "
    message_str = "> 📝 *Average time taken for*:\n"
    message_str += f">{white_space}- *All eddie responses*: {seconds_to_readable_time(avg_all_res_in_ms/1000)}\n"
    message_str += f">{white_space}- *First eddie response for new users*: {seconds_to_readable_time(avg_first_res_in_ms/1000)}\n"
    return message_str


def handle_daily_prompting_time_report():
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"⏰ ({formated_date}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            print(
                f"Daily report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing daily prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing daily prompting time report*"
    print("Building slack message for daily prompting time report...")
    return build_slack_template(header, message)


def handle_weekly_prompting_time_report():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"⏰ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            print(
                f"Weekly report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing weekly prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing weekly prompting time report*"
    print("Building slack message for weekly prompting time report...")
    return build_slack_template(header, message)
import asyncio
import base64
import gzip
import json
import os
import zipfile

from datetime import datetime, timedelta
from typing import Dict, List
from config import (
    AMPLITUDE_API_KEY,
    AMPLITUDE_SECRET_KEY,
    AMPLITUDE_PROJECT_ID,
    EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
)
from models.export_model import ExportInfo
from models.metrics_model import MetricsData, ReportMetrics
from slack_sdk.models.blocks import SectionBlock, HeaderBlock, DividerBlock
from services.auth0.auth0_service import get_auth_token, search_auth0_users
from models.project_model import ProjectInfo
from models.user_activity_model import UserActivity, UserActivityStore
from services.slack.send_message import send_slack_in_thread_message, send_slack_message
from utils import get_start_end_of_week_by_offset
from services.slack.message_templates import build_slack_template
from utils import (
    format_bytes,
    free_up_disk_space,
    format_percentage_change,
    seconds_to_readable_time,
)
from enums import AmplitudeEvents, AmplitudeQueryMetrics, MetricCategory, MetricsName
from services.amplitude.amplitude_service import (
    export_amplitude_events,
    sum_filtered_amplitude_events,
)

SLACK_MESSAGE_LIMIT = 3000
EXPORT_FILENAME = "export_data"
# Save in /tmp to make it writable in AWS Lambda.
# https://repost.aws/questions/QUyYQzTTPnRY6_2w71qscojA/read-only-file-system-aws-lambda
EXPORT_FILEPATH = f"/tmp/{EXPORT_FILENAME}"
EXTRACTED_DATA_FILEPATH = f"/tmp/{EXPORT_FILENAME}/{AMPLITUDE_PROJECT_ID}/"


def build_analytics_message(
    metrics_data: ReportMetrics, prev_period_metrics_data: ReportMetrics
) -> str:
    str_msg = ""
    for metric_type, metric_data in metrics_data.data.items():
        str_msg += f"\n>*{metric_type.value}:*"
        for metric_name, value in metric_data.metrics.items():
            whitespace = "          "

            if not value and value != 0:
                str_msg += f"\n>f{whitespace}- *{metric_name}:* No data found"
                continue

            prev_period_value = None
            prev_period_data: MetricsData = prev_period_metrics_data.data.get(
                metric_type, None
            )
            if prev_period_data:
                prev_period_value = prev_period_data.metrics.get(metric_name, None)
            print(
                f"[{metric_type.value}]Comparing metrics [{metric_name}]: ",
                value,
                prev_period_value,
            )

            compare_str = (
                "(No data for previous period)"
                if not prev_period_value and prev_period_value != 0
                else format_percentage_change(value, prev_period_value)
            )
            if metric_name == MetricsName.total_video_duration:
                value = seconds_to_readable_time(value)
            elif metric_name in [
                MetricsName.export_mp4_landscape_with_captions,
                MetricsName.export_mp4_landscape_without_captions,
                MetricsName.export_mp4_vertical_with_captions,
                MetricsName.export_mp4_vertical_without_captions,
            ]:
                whitespace *= 2

            str_msg += f"\n>{whitespace}- *{metric_name}:* {value} {compare_str}"
    return str_msg


def handle_daily_lambda_event():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    comparison_date = today - timedelta(days=2)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formated_date}) Eddie Daily Analytics"

    try:
        # Query data for daily report
        daily_metrics_data = get_analytics_metrics_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if not daily_metrics_data:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            # Query data for compare with daily report
            prev_period_metrics_data = get_analytics_metrics_data(
                comparison_date.strftime("%Y%m%d"), comparison_date.strftime("%Y%m%d")
            )
            print("Daily: ", daily_metrics_data)
            print("Compare with: ", prev_period_metrics_data)
            message = build_analytics_message(
                daily_metrics_data, prev_period_metrics_data
            )
    except Exception as e:
        print("Unexpected error occur in daily event --errors: ", e)
        message = f"> - *Error when preparing message for daily analytics*"
    print("Building slack message for daily eddie analytics...")
    return build_slack_template(header, message)


def handle_weekly_lambda_event():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    (
        monday_of_comparison_week,
        sunday_of_comparison_week,
    ) = get_start_end_of_week_by_offset(week_offset=2)

    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Weekly Analytics"

    try:
        weekly_metrics_data = get_analytics_metrics_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )

        if not weekly_metrics_data:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            prev_period_metrics_data = get_analytics_metrics_data(
                monday_of_comparison_week.strftime("%Y%m%d"),
                sunday_of_comparison_week.strftime("%Y%m%d"),
            )
            message = build_analytics_message(
                weekly_metrics_data, prev_period_metrics_data
            )

    except Exception as e:
        print("Unexpected error occur in weekly event --errors: ", e)
        message = f"> - *Error when preparing message for weekly analytics*"
    print("Building slack message for weekly eddie analytics...")
    return build_slack_template(header, message)


def get_analytics_metrics_data(start_date, end_date):
    # date params are in format str YYYYMMDD
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    project_creation_metrics = {}
    user_activity_metrics = {}
    share_acitivity_metrics = {}
    exports_metrics = {}

    # Project creation:
    # how many single cam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["singlecam"],
            }
        ],
    }

    singlecam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.singlecam_project_created: singlecam_project_created}
    )

    # how many multicam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["multicam"],
            }
        ],
    }
    multicam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.multicam_project_created: multicam_project_created}
    )

    # How many project created via web?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["browser"],
            }
        ],
    }

    project_created_by_web = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_web: project_created_by_web}
    )

    # How many project created via desktop?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["desktop"],
            }
        ],
    }

    project_created_by_desktop_app = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_desktop_app: project_created_by_desktop_app}
    )

    # User Activity:
    # How many new users?
    start_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    params = {
        "include_totals": "true",
        # auth0 require date in format YYYY-MM-DD
        "q": f"created_at:[{start_date_formatted} TO {end_date_formatted}]",
    }
    access_token = get_auth_token()
    response = search_auth0_users(params, access_token)
    new_user = response.get("total", 0)
    user_activity_metrics.update({MetricsName.new_user: new_user})

    # how many videos were uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_uploaded: file_were_uploaded})

    # how many hours of video uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
        "group_by": [
            {
                "type": "event",
                "value": "durationSec",
            }
        ],
    }
    total_video_duration = sum_filtered_amplitude_events(
        event_filters, start_date, end_date, AmplitudeQueryMetrics.sums, AMPLITUDE_AUTH
    )
    user_activity_metrics.update(
        {MetricsName.total_video_duration: total_video_duration}
    )

    # Unsuccessful Upload Attempts: (Unsupported language or no speech detected)
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_failed,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_upload_failed: file_were_uploaded})

    # how many returning users (users who already signed up who came back this week and created a new project)
    event_filters = {
        "event_type": AmplitudeEvents.returning_user,
    }
    returning_users = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.returning_user: returning_users})

    # Sharing Activity:
    # How many share links opened?
    event_filters = {
        "event_type": AmplitudeEvents.open_share_link,
    }
    share_links_opened = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.open_share_link: share_links_opened})

    # How many share links created?
    event_filters = {
        "event_type": AmplitudeEvents.create_share_link,
    }
    share_links_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.create_share_link: share_links_created})

    # Exports:
    # Total Export Mp4(Landscape)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions", "original"],
            },
        ],
    }

    total_export_lanscape = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_landscape: total_export_lanscape})

    # Export Mp4(Landscape) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions"],
            },
        ],
    }

    export_landscape_without_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {
            MetricsName.export_mp4_landscape_without_captions: export_landscape_without_captions
        }
    )

    # Export Mp4(Landscape) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original"],
            },
        ],
    }

    export_landscape_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_landscape_with_captions: export_landscape_with_captions}
    )

    # Total Export Mp4(Vertical)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions", "vertical"],
            },
        ],
    }

    total_export_vertical = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_vertical: total_export_vertical})

    # Export Mp4(Vertical) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions"],
            },
        ],
    }

    vertical_no_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_without_captions: vertical_no_captions}
    )

    # Export Mp4(Vertical) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical"],
            },
        ],
    }

    vertical_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_with_captions: vertical_with_captions}
    )

    # how many exports adobe premiere
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["premiere"],
            }
        ],
    }

    export_premiere = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_adobe: export_premiere})

    # how many exports davinci resolve
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["davinci"],
            }
        ],
    }

    export_davinci = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_resolve: export_davinci})

    # how many exports to fcp
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["fcpxml"],
            }
        ],
    }
    export_fcp = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_fcp: export_fcp})

    # how many exports (total)
    event_filters = {"event_type": AmplitudeEvents.user_click_export}
    total_exports = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.total_exports: total_exports})

    return ReportMetrics(
        data={
            MetricCategory.project_creation: MetricsData(
                metrics=project_creation_metrics
            ),
            MetricCategory.user_activity: MetricsData(metrics=user_activity_metrics),
            MetricCategory.share_activity: MetricsData(metrics=share_acitivity_metrics),
            MetricCategory.export: MetricsData(metrics=exports_metrics),
        }
    )


def parse_export_data(extracted_file_paths: List[str]) -> List[UserActivity]:
    # Summarize user activity
    user_store = UserActivityStore()

    for json_file in extracted_file_paths:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            for index, event in enumerate(data, start=1):
                if event.get("data_type") != "event":
                    continue

                user_id = (
                    event.get("user_id")
                    or f'Anonymous (Amplitude ID-{event.get("amplitude_id", index)})'
                )
                user_activity = user_store.get_activity(user_id)
                project = handle_export_event(event, user_activity)
                if project:
                    user_activity.add_project(project)
                    user_store.add_activity(user_activity)
    return user_store.get_all_activities()


def handle_export_event(event, user_activity: UserActivity):
    event_type = event.get("event_type")
    event_properties = event.get("event_properties")
    project_id = event_properties.get("projectId")
    project_name = event_properties.get("projectName")

    try:
        raw_files_info = event_properties.get("projectFileInfo", "[]")
        files_info = (
            json.loads(raw_files_info)
            if isinstance(raw_files_info, str)
            else raw_files_info
        )

        project_files_info = (
            ", ".join(
                [
                    f"{file.get('filename')} ({format_bytes(int(file.get('filesize', 0)))})"
                    for file in files_info
                ]
            )
            if files_info and isinstance(files_info, list)
            else None
        )
    except (json.JSONDecodeError, TypeError) as e:
        project_files_info = None
        print(f"Error parsing projectFileInfo: {e}")

    if event_type in {
        AmplitudeEvents.user_submit_prompt,
        AmplitudeEvents.user_click_prompt_suggest,
    }:
        prompt = (
            event_properties.get("prompt")
            if event_type == AmplitudeEvents.user_submit_prompt
            else event_properties.get("promptSuggest")
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_prompt(prompt)
        else:
            # Init new project with prompt
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[prompt],
                exports=[],
            )
        return project

    elif event_type == AmplitudeEvents.file_export_succeeded:
        layout = event_properties.get("layout")
        export_type = event_properties.get("exportType")
        ouput_duration_str = event_properties.get("outputDuration", None)
        readable_duration = (
            seconds_to_readable_time(float(ouput_duration_str))
            if ouput_duration_str
            else None
        )
        export_filename = event_properties.get("exportFileName")
        export_info = ExportInfo(
            export_layout=layout,
            export_type=export_type,
            export_file_name=export_filename,
            output_duration=readable_duration,
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_export(export_info)
        else:
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[],
                exports=[export_info],
            )
        return project
    else:
        return None


def handle_export_data_response(export_data) -> List[str]:
    os.makedirs(os.path.dirname(EXTRACTED_DATA_FILEPATH), exist_ok=True)
    # Write export data as zip
    with open(f"{EXPORT_FILEPATH}.zip", "wb") as f:
        f.write(export_data.content)
    # Extract data from zip archive
    with zipfile.ZipFile(f"{EXPORT_FILEPATH}.zip", "r") as zip_ref:
        zip_ref.extractall(f"/tmp/{EXPORT_FILENAME}")

    return os.listdir(EXTRACTED_DATA_FILEPATH)


def extract_export_data(files: List[str]) -> List[str]:
    ouput_file_paths = []

    # Extract export data to json
    for file in files:
        if file.endswith(".json.gz"):
            file_path = os.path.join(EXTRACTED_DATA_FILEPATH, file)

            extracted_data = []
            with gzip.open(file_path, "rt", encoding="utf-8") as gz_file:
                for line in gz_file:
                    json_obj = json.loads(line.strip())
                    extracted_data.append(json_obj)

            output_file = file.replace(".gz", "")
            output_path = os.path.join(EXTRACTED_DATA_FILEPATH, output_file)

            with open(output_path, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file, indent=4)

            ouput_file_paths.append(EXTRACTED_DATA_FILEPATH + output_file)
            print(f"Extracted: {output_file}")
    return ouput_file_paths


async def handle_send_daily_users(
    date: str, user_store: List[UserActivity]
) -> Dict[str, str]:
    # Send report header
    header = f"🗓️ ({date}) Daily User Interactions\n"

    await send_slack_in_thread_message(
        {
            "blocks": [
                HeaderBlock(text=f"{header}").to_dict(),
                SectionBlock(text=" ").to_dict(),
            ]
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )

    users_thread = {}
    # Send user email message and init thread
    for idx, user_activity in enumerate(user_store, start=1):
        message = f"{idx}) 👤User: {user_activity.email}\n"
        # chat.postMessage has special rate limiting conditions (1msg/s)
        # https://api.slack.com/methods/chat.postMessage#rate_limiting
        await asyncio.sleep(1)
        res = await send_slack_in_thread_message(
            {"text": message}, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        users_thread[user_activity.email] = res.get("ts")

    # Send report footer
    await send_slack_in_thread_message(
        {
            "blocks": [
                SectionBlock(
                    text=f":checkered_flag: *End of Report:* {header}"
                ).to_dict(),
                SectionBlock(text=" ").to_dict(),
                DividerBlock(type="divider").to_dict(),
            ],
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )
    return users_thread


async def handle_send_message_by_thread(
    users_thread: Dict[str, str], user_store: List[UserActivity]
):
    try:
        whitespace = "        "

        for idx, user_activity in enumerate(user_store, start=1):
            parsed_str = []

            for project_idx, project in enumerate(
                user_activity.projects.values(), start=1
            ):
                project_line = f":movie_camera: #{project_idx}. *Project name:* {project.project_name}\n"
                parsed_str.append(f"{project_line}")

                if project.file_info:
                    parsed_str.append(
                        f"{whitespace*2}- *File info:* {project.file_info}\n"
                    )

                if len(project.prompts) > 0:
                    parsed_str.append(f"{whitespace}:man-raising-hand: *Prompts:*\n")
                    for prompt_idx, prompt in enumerate(project.prompts, start=1):
                        parsed_str.append(f"{whitespace*2} - #{prompt_idx}: {prompt}\n")

                if len(project.exports) > 0:
                    parsed_str.append(f"{whitespace}:inbox_tray: *Exports:*\n")
                    for export_idx, export in enumerate(project.exports, start=1):
                        export_msg = (
                            f"{whitespace*2} - #{export_idx}:  Format: {export.export_layout}-{export.export_type}"
                            if export.export_layout
                            else f"{whitespace*2} - #{export_idx}:  Format: {export.export_type}"
                        )
                        if export.output_duration:
                            export_msg = (
                                f"{export_msg}, duration: {export.output_duration}"
                            )
                        parsed_str.append(f"{export_msg}\n")

            message_str = "".join(parsed_str)
            i = 0
            while i < len(message_str):
                end_index = min(i + SLACK_MESSAGE_LIMIT, len(message_str))
                if end_index < len(message_str) and "\n" in message_str[i:end_index]:
                    end_index = message_str.rfind("\n", i, end_index) + 1

                message = message_str[i:end_index]
                # chat.postMessage has special rate limiting conditions (1msg/s)
                # https://api.slack.com/methods/chat.postMessage#rate_limiting
                await asyncio.sleep(1)
                await send_slack_in_thread_message(
                    message={"text": message},
                    channel_id=EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
                    thread_ts=users_thread[user_activity.email],
                )
                i = end_index

    except Exception as e:
        print(f"Error handle_send_message_by_thread --errors: '{e}'")
        raise


async def handle_summarizes_user_interactions():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.strftime("%Y%m%d") + "T00"
    end_date = yesterday.strftime("%Y%m%d") + "T23"
    # format to readable date for send slack message
    formatted_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_date}) Daily User Interactions\n"

    export_data = export_amplitude_events(start_date, end_date)
    if not export_data:
        message = f"> - *Error when exporting events for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        return

    try:
        ouptut_file_paths = handle_export_data_response(export_data)
        extracted_file_paths = extract_export_data(ouptut_file_paths)
        print(
            "Exported data, preparing data for daily summarizes user interactions analytics..."
        )
        data = parse_export_data(extracted_file_paths)
        print("Sending slack messsage...")
        if len(data) == 0:
            message = f"> - *Export data not found*"
            slack_template = build_slack_template(header, message)
            await send_slack_in_thread_message(
                slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
            )
        else:
            users_thread = await handle_send_daily_users(formatted_date, data)
            await handle_send_message_by_thread(users_thread, data)
    except Exception as e:
        print(
            "Unexpected error occur in daily summarizes user interactions event --errors: ",
            e,
        )
        message = f"> - *Error when preparing data for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
    finally:
        print("Sent slack message for daily summarizes user interactions analytics")
        free_up_disk_space(f"{EXPORT_FILEPATH}.zip")
        free_up_disk_space(EXTRACTED_DATA_FILEPATH)


def get_prompting_time_data(start_date, end_date) -> tuple:
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    # Average time taken for first eddie response for all users (Avarage the first time time taken when a new user prompts.)
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "filters": [
            {
                "subprop_type": "nth_time_hack",
                "subprop_key": "nth_time_performed",
                "subprop_op": "is",
                "subprop_value": ["1"],
            }
        ],
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_first_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )

    # Average time taken for all eddie responses
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_all_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )
    return avg_first_res_in_ms, avg_all_res_in_ms


def build_prompting_time_message(avg_first_res_in_ms, avg_all_res_in_ms):
    white_space = "            "
    message_str = "> 📝 *Average time taken for*:\n"
    message_str += f">{white_space}- *All eddie responses*: {seconds_to_readable_time(avg_all_res_in_ms/1000)}\n"
    message_str += f">{white_space}- *First eddie response for new users*: {seconds_to_readable_time(avg_first_res_in_ms/1000)}\n"
    return message_str


def handle_daily_prompting_time_report():
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"⏰ ({formated_date}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            print(
                f"Daily report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing daily prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing daily prompting time report*"
    print("Building slack message for daily prompting time report...")
    return build_slack_template(header, message)


def handle_weekly_prompting_time_report():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"⏰ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            print(
                f"Weekly report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing weekly prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing weekly prompting time report*"
    print("Building slack message for weekly prompting time report...")
    return build_slack_template(header, message)
import asyncio
import base64
import gzip
import json
import os
import zipfile

from datetime import datetime, timedelta
from typing import Dict, List
from config import (
    AMPLITUDE_API_KEY,
    AMPLITUDE_SECRET_KEY,
    AMPLITUDE_PROJECT_ID,
    EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
)
from models.export_model import ExportInfo
from models.metrics_model import MetricsData, ReportMetrics
from slack_sdk.models.blocks import SectionBlock, HeaderBlock, DividerBlock
from services.auth0.auth0_service import get_auth_token, search_auth0_users
from models.project_model import ProjectInfo
from models.user_activity_model import UserActivity, UserActivityStore
from services.slack.send_message import send_slack_in_thread_message, send_slack_message
from utils import get_start_end_of_week_by_offset
from services.slack.message_templates import build_slack_template
from utils import (
    format_bytes,
    free_up_disk_space,
    format_percentage_change,
    seconds_to_readable_time,
)
from enums import AmplitudeEvents, AmplitudeQueryMetrics, MetricCategory, MetricsName
from services.amplitude.amplitude_service import (
    export_amplitude_events,
    sum_filtered_amplitude_events,
)

SLACK_MESSAGE_LIMIT = 3000
EXPORT_FILENAME = "export_data"
# Save in /tmp to make it writable in AWS Lambda.
# https://repost.aws/questions/QUyYQzTTPnRY6_2w71qscojA/read-only-file-system-aws-lambda
EXPORT_FILEPATH = f"/tmp/{EXPORT_FILENAME}"
EXTRACTED_DATA_FILEPATH = f"/tmp/{EXPORT_FILENAME}/{AMPLITUDE_PROJECT_ID}/"


def build_analytics_message(
    metrics_data: ReportMetrics, prev_period_metrics_data: ReportMetrics
) -> str:
    str_msg = ""
    for metric_type, metric_data in metrics_data.data.items():
        str_msg += f"\n>*{metric_type.value}:*"
        for metric_name, value in metric_data.metrics.items():
            whitespace = "          "

            if not value and value != 0:
                str_msg += f"\n>f{whitespace}- *{metric_name}:* No data found"
                continue

            prev_period_value = None
            prev_period_data: MetricsData = prev_period_metrics_data.data.get(
                metric_type, None
            )
            if prev_period_data:
                prev_period_value = prev_period_data.metrics.get(metric_name, None)
            print(
                f"[{metric_type.value}]Comparing metrics [{metric_name}]: ",
                value,
                prev_period_value,
            )

            compare_str = (
                "(No data for previous period)"
                if not prev_period_value and prev_period_value != 0
                else format_percentage_change(value, prev_period_value)
            )
            if metric_name == MetricsName.total_video_duration:
                value = seconds_to_readable_time(value)
            elif metric_name in [
                MetricsName.export_mp4_landscape_with_captions,
                MetricsName.export_mp4_landscape_without_captions,
                MetricsName.export_mp4_vertical_with_captions,
                MetricsName.export_mp4_vertical_without_captions,
            ]:
                whitespace *= 2

            str_msg += f"\n>{whitespace}- *{metric_name}:* {value} {compare_str}"
    return str_msg


def handle_daily_lambda_event():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    comparison_date = today - timedelta(days=2)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formated_date}) Eddie Daily Analytics"

    try:
        # Query data for daily report
        daily_metrics_data = get_analytics_metrics_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if not daily_metrics_data:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            # Query data for compare with daily report
            prev_period_metrics_data = get_analytics_metrics_data(
                comparison_date.strftime("%Y%m%d"), comparison_date.strftime("%Y%m%d")
            )
            print("Daily: ", daily_metrics_data)
            print("Compare with: ", prev_period_metrics_data)
            message = build_analytics_message(
                daily_metrics_data, prev_period_metrics_data
            )
    except Exception as e:
        print("Unexpected error occur in daily event --errors: ", e)
        message = f"> - *Error when preparing message for daily analytics*"
    print("Building slack message for daily eddie analytics...")
    return build_slack_template(header, message)


def handle_weekly_lambda_event():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    (
        monday_of_comparison_week,
        sunday_of_comparison_week,
    ) = get_start_end_of_week_by_offset(week_offset=2)

    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Weekly Analytics"

    try:
        weekly_metrics_data = get_analytics_metrics_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )

        if not weekly_metrics_data:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            prev_period_metrics_data = get_analytics_metrics_data(
                monday_of_comparison_week.strftime("%Y%m%d"),
                sunday_of_comparison_week.strftime("%Y%m%d"),
            )
            message = build_analytics_message(
                weekly_metrics_data, prev_period_metrics_data
            )

    except Exception as e:
        print("Unexpected error occur in weekly event --errors: ", e)
        message = f"> - *Error when preparing message for weekly analytics*"
    print("Building slack message for weekly eddie analytics...")
    return build_slack_template(header, message)


def get_analytics_metrics_data(start_date, end_date):
    # date params are in format str YYYYMMDD
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    project_creation_metrics = {}
    user_activity_metrics = {}
    share_acitivity_metrics = {}
    exports_metrics = {}

    # Project creation:
    # how many single cam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["singlecam"],
            }
        ],
    }

    singlecam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.singlecam_project_created: singlecam_project_created}
    )

    # how many multicam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["multicam"],
            }
        ],
    }
    multicam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.multicam_project_created: multicam_project_created}
    )

    # How many project created via web?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["browser"],
            }
        ],
    }

    project_created_by_web = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_web: project_created_by_web}
    )

    # How many project created via desktop?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["desktop"],
            }
        ],
    }

    project_created_by_desktop_app = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_desktop_app: project_created_by_desktop_app}
    )

    # User Activity:
    # How many new users?
    start_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    params = {
        "include_totals": "true",
        # auth0 require date in format YYYY-MM-DD
        "q": f"created_at:[{start_date_formatted} TO {end_date_formatted}]",
    }
    access_token = get_auth_token()
    response = search_auth0_users(params, access_token)
    new_user = response.get("total", 0)
    user_activity_metrics.update({MetricsName.new_user: new_user})

    # how many videos were uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_uploaded: file_were_uploaded})

    # how many hours of video uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
        "group_by": [
            {
                "type": "event",
                "value": "durationSec",
            }
        ],
    }
    total_video_duration = sum_filtered_amplitude_events(
        event_filters, start_date, end_date, AmplitudeQueryMetrics.sums, AMPLITUDE_AUTH
    )
    user_activity_metrics.update(
        {MetricsName.total_video_duration: total_video_duration}
    )

    # Unsuccessful Upload Attempts: (Unsupported language or no speech detected)
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_failed,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_upload_failed: file_were_uploaded})

    # how many returning users (users who already signed up who came back this week and created a new project)
    event_filters = {
        "event_type": AmplitudeEvents.returning_user,
    }
    returning_users = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.returning_user: returning_users})

    # Sharing Activity:
    # How many share links opened?
    event_filters = {
        "event_type": AmplitudeEvents.open_share_link,
    }
    share_links_opened = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.open_share_link: share_links_opened})

    # How many share links created?
    event_filters = {
        "event_type": AmplitudeEvents.create_share_link,
    }
    share_links_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.create_share_link: share_links_created})

    # Exports:
    # Total Export Mp4(Landscape)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions", "original"],
            },
        ],
    }

    total_export_lanscape = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_landscape: total_export_lanscape})

    # Export Mp4(Landscape) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions"],
            },
        ],
    }

    export_landscape_without_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {
            MetricsName.export_mp4_landscape_without_captions: export_landscape_without_captions
        }
    )

    # Export Mp4(Landscape) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original"],
            },
        ],
    }

    export_landscape_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_landscape_with_captions: export_landscape_with_captions}
    )

    # Total Export Mp4(Vertical)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions", "vertical"],
            },
        ],
    }

    total_export_vertical = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_vertical: total_export_vertical})

    # Export Mp4(Vertical) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions"],
            },
        ],
    }

    vertical_no_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_without_captions: vertical_no_captions}
    )

    # Export Mp4(Vertical) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical"],
            },
        ],
    }

    vertical_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_with_captions: vertical_with_captions}
    )

    # how many exports adobe premiere
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["premiere"],
            }
        ],
    }

    export_premiere = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_adobe: export_premiere})

    # how many exports davinci resolve
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["davinci"],
            }
        ],
    }

    export_davinci = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_resolve: export_davinci})

    # how many exports to fcp
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["fcpxml"],
            }
        ],
    }
    export_fcp = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_fcp: export_fcp})

    # how many exports (total)
    event_filters = {"event_type": AmplitudeEvents.user_click_export}
    total_exports = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.total_exports: total_exports})

    return ReportMetrics(
        data={
            MetricCategory.project_creation: MetricsData(
                metrics=project_creation_metrics
            ),
            MetricCategory.user_activity: MetricsData(metrics=user_activity_metrics),
            MetricCategory.share_activity: MetricsData(metrics=share_acitivity_metrics),
            MetricCategory.export: MetricsData(metrics=exports_metrics),
        }
    )


def parse_export_data(extracted_file_paths: List[str]) -> List[UserActivity]:
    # Summarize user activity
    user_store = UserActivityStore()

    for json_file in extracted_file_paths:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            for index, event in enumerate(data, start=1):
                if event.get("data_type") != "event":
                    continue

                user_id = (
                    event.get("user_id")
                    or f'Anonymous (Amplitude ID-{event.get("amplitude_id", index)})'
                )
                user_activity = user_store.get_activity(user_id)
                project = handle_export_event(event, user_activity)
                if project:
                    user_activity.add_project(project)
                    user_store.add_activity(user_activity)
    return user_store.get_all_activities()


def handle_export_event(event, user_activity: UserActivity):
    event_type = event.get("event_type")
    event_properties = event.get("event_properties")
    project_id = event_properties.get("projectId")
    project_name = event_properties.get("projectName")

    try:
        raw_files_info = event_properties.get("projectFileInfo", "[]")
        files_info = (
            json.loads(raw_files_info)
            if isinstance(raw_files_info, str)
            else raw_files_info
        )

        project_files_info = (
            ", ".join(
                [
                    f"{file.get('filename')} ({format_bytes(int(file.get('filesize', 0)))})"
                    for file in files_info
                ]
            )
            if files_info and isinstance(files_info, list)
            else None
        )
    except (json.JSONDecodeError, TypeError) as e:
        project_files_info = None
        print(f"Error parsing projectFileInfo: {e}")

    if event_type in {
        AmplitudeEvents.user_submit_prompt,
        AmplitudeEvents.user_click_prompt_suggest,
    }:
        prompt = (
            event_properties.get("prompt")
            if event_type == AmplitudeEvents.user_submit_prompt
            else event_properties.get("promptSuggest")
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_prompt(prompt)
        else:
            # Init new project with prompt
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[prompt],
                exports=[],
            )
        return project

    elif event_type == AmplitudeEvents.file_export_succeeded:
        layout = event_properties.get("layout")
        export_type = event_properties.get("exportType")
        ouput_duration_str = event_properties.get("outputDuration", None)
        readable_duration = (
            seconds_to_readable_time(float(ouput_duration_str))
            if ouput_duration_str
            else None
        )
        export_filename = event_properties.get("exportFileName")
        export_info = ExportInfo(
            export_layout=layout,
            export_type=export_type,
            export_file_name=export_filename,
            output_duration=readable_duration,
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_export(export_info)
        else:
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[],
                exports=[export_info],
            )
        return project
    else:
        return None


def handle_export_data_response(export_data) -> List[str]:
    os.makedirs(os.path.dirname(EXTRACTED_DATA_FILEPATH), exist_ok=True)
    # Write export data as zip
    with open(f"{EXPORT_FILEPATH}.zip", "wb") as f:
        f.write(export_data.content)
    # Extract data from zip archive
    with zipfile.ZipFile(f"{EXPORT_FILEPATH}.zip", "r") as zip_ref:
        zip_ref.extractall(f"/tmp/{EXPORT_FILENAME}")

    return os.listdir(EXTRACTED_DATA_FILEPATH)


def extract_export_data(files: List[str]) -> List[str]:
    ouput_file_paths = []

    # Extract export data to json
    for file in files:
        if file.endswith(".json.gz"):
            file_path = os.path.join(EXTRACTED_DATA_FILEPATH, file)

            extracted_data = []
            with gzip.open(file_path, "rt", encoding="utf-8") as gz_file:
                for line in gz_file:
                    json_obj = json.loads(line.strip())
                    extracted_data.append(json_obj)

            output_file = file.replace(".gz", "")
            output_path = os.path.join(EXTRACTED_DATA_FILEPATH, output_file)

            with open(output_path, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file, indent=4)

            ouput_file_paths.append(EXTRACTED_DATA_FILEPATH + output_file)
            print(f"Extracted: {output_file}")
    return ouput_file_paths


async def handle_send_daily_users(
    date: str, user_store: List[UserActivity]
) -> Dict[str, str]:
    # Send report header
    header = f"🗓️ ({date}) Daily User Interactions\n"

    await send_slack_in_thread_message(
        {
            "blocks": [
                HeaderBlock(text=f"{header}").to_dict(),
                SectionBlock(text=" ").to_dict(),
            ]
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )

    users_thread = {}
    # Send user email message and init thread
    for idx, user_activity in enumerate(user_store, start=1):
        message = f"{idx}) 👤User: {user_activity.email}\n"
        # chat.postMessage has special rate limiting conditions (1msg/s)
        # https://api.slack.com/methods/chat.postMessage#rate_limiting
        await asyncio.sleep(1)
        res = await send_slack_in_thread_message(
            {"text": message}, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        users_thread[user_activity.email] = res.get("ts")

    # Send report footer
    await send_slack_in_thread_message(
        {
            "blocks": [
                SectionBlock(
                    text=f":checkered_flag: *End of Report:* {header}"
                ).to_dict(),
                SectionBlock(text=" ").to_dict(),
                DividerBlock(type="divider").to_dict(),
            ],
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )
    return users_thread


async def handle_send_message_by_thread(
    users_thread: Dict[str, str], user_store: List[UserActivity]
):
    try:
        whitespace = "        "

        for idx, user_activity in enumerate(user_store, start=1):
            parsed_str = []

            for project_idx, project in enumerate(
                user_activity.projects.values(), start=1
            ):
                project_line = f":movie_camera: #{project_idx}. *Project name:* {project.project_name}\n"
                parsed_str.append(f"{project_line}")

                if project.file_info:
                    parsed_str.append(
                        f"{whitespace*2}- *File info:* {project.file_info}\n"
                    )

                if len(project.prompts) > 0:
                    parsed_str.append(f"{whitespace}:man-raising-hand: *Prompts:*\n")
                    for prompt_idx, prompt in enumerate(project.prompts, start=1):
                        parsed_str.append(f"{whitespace*2} - #{prompt_idx}: {prompt}\n")

                if len(project.exports) > 0:
                    parsed_str.append(f"{whitespace}:inbox_tray: *Exports:*\n")
                    for export_idx, export in enumerate(project.exports, start=1):
                        export_msg = (
                            f"{whitespace*2} - #{export_idx}:  Format: {export.export_layout}-{export.export_type}"
                            if export.export_layout
                            else f"{whitespace*2} - #{export_idx}:  Format: {export.export_type}"
                        )
                        if export.output_duration:
                            export_msg = (
                                f"{export_msg}, duration: {export.output_duration}"
                            )
                        parsed_str.append(f"{export_msg}\n")

            message_str = "".join(parsed_str)
            i = 0
            while i < len(message_str):
                end_index = min(i + SLACK_MESSAGE_LIMIT, len(message_str))
                if end_index < len(message_str) and "\n" in message_str[i:end_index]:
                    end_index = message_str.rfind("\n", i, end_index) + 1

                message = message_str[i:end_index]
                # chat.postMessage has special rate limiting conditions (1msg/s)
                # https://api.slack.com/methods/chat.postMessage#rate_limiting
                await asyncio.sleep(1)
                await send_slack_in_thread_message(
                    message={"text": message},
                    channel_id=EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
                    thread_ts=users_thread[user_activity.email],
                )
                i = end_index

    except Exception as e:
        print(f"Error handle_send_message_by_thread --errors: '{e}'")
        raise


async def handle_summarizes_user_interactions():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.strftime("%Y%m%d") + "T00"
    end_date = yesterday.strftime("%Y%m%d") + "T23"
    # format to readable date for send slack message
    formatted_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_date}) Daily User Interactions\n"

    export_data = export_amplitude_events(start_date, end_date)
    if not export_data:
        message = f"> - *Error when exporting events for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        return

    try:
        ouptut_file_paths = handle_export_data_response(export_data)
        extracted_file_paths = extract_export_data(ouptut_file_paths)
        print(
            "Exported data, preparing data for daily summarizes user interactions analytics..."
        )
        data = parse_export_data(extracted_file_paths)
        print("Sending slack messsage...")
        if len(data) == 0:
            message = f"> - *Export data not found*"
            slack_template = build_slack_template(header, message)
            await send_slack_in_thread_message(
                slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
            )
        else:
            users_thread = await handle_send_daily_users(formatted_date, data)
            await handle_send_message_by_thread(users_thread, data)
    except Exception as e:
        print(
            "Unexpected error occur in daily summarizes user interactions event --errors: ",
            e,
        )
        message = f"> - *Error when preparing data for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
    finally:
        print("Sent slack message for daily summarizes user interactions analytics")
        free_up_disk_space(f"{EXPORT_FILEPATH}.zip")
        free_up_disk_space(EXTRACTED_DATA_FILEPATH)


def get_prompting_time_data(start_date, end_date) -> tuple:
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    # Average time taken for first eddie response for all users (Avarage the first time time taken when a new user prompts.)
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "filters": [
            {
                "subprop_type": "nth_time_hack",
                "subprop_key": "nth_time_performed",
                "subprop_op": "is",
                "subprop_value": ["1"],
            }
        ],
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_first_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )

    # Average time taken for all eddie responses
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_all_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )
    return avg_first_res_in_ms, avg_all_res_in_ms


def build_prompting_time_message(avg_first_res_in_ms, avg_all_res_in_ms):
    white_space = "            "
    message_str = "> 📝 *Average time taken for*:\n"
    message_str += f">{white_space}- *All eddie responses*: {seconds_to_readable_time(avg_all_res_in_ms/1000)}\n"
    message_str += f">{white_space}- *First eddie response for new users*: {seconds_to_readable_time(avg_first_res_in_ms/1000)}\n"
    return message_str


def handle_daily_prompting_time_report():
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"⏰ ({formated_date}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            print(
                f"Daily report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing daily prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing daily prompting time report*"
    print("Building slack message for daily prompting time report...")
    return build_slack_template(header, message)


def handle_weekly_prompting_time_report():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"⏰ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            print(
                f"Weekly report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing weekly prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing weekly prompting time report*"
    print("Building slack message for weekly prompting time report...")
    return build_slack_template(header, message)
import asyncio
import base64
import gzip
import json
import os
import zipfile

from datetime import datetime, timedelta
from typing import Dict, List
from config import (
    AMPLITUDE_API_KEY,
    AMPLITUDE_SECRET_KEY,
    AMPLITUDE_PROJECT_ID,
    EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
)
from models.export_model import ExportInfo
from models.metrics_model import MetricsData, ReportMetrics
from slack_sdk.models.blocks import SectionBlock, HeaderBlock, DividerBlock
from services.auth0.auth0_service import get_auth_token, search_auth0_users
from models.project_model import ProjectInfo
from models.user_activity_model import UserActivity, UserActivityStore
from services.slack.send_message import send_slack_in_thread_message, send_slack_message
from utils import get_start_end_of_week_by_offset
from services.slack.message_templates import build_slack_template
from utils import (
    format_bytes,
    free_up_disk_space,
    format_percentage_change,
    seconds_to_readable_time,
)
from enums import AmplitudeEvents, AmplitudeQueryMetrics, MetricCategory, MetricsName
from services.amplitude.amplitude_service import (
    export_amplitude_events,
    sum_filtered_amplitude_events,
)

SLACK_MESSAGE_LIMIT = 3000
EXPORT_FILENAME = "export_data"
# Save in /tmp to make it writable in AWS Lambda.
# https://repost.aws/questions/QUyYQzTTPnRY6_2w71qscojA/read-only-file-system-aws-lambda
EXPORT_FILEPATH = f"/tmp/{EXPORT_FILENAME}"
EXTRACTED_DATA_FILEPATH = f"/tmp/{EXPORT_FILENAME}/{AMPLITUDE_PROJECT_ID}/"


def build_analytics_message(
    metrics_data: ReportMetrics, prev_period_metrics_data: ReportMetrics
) -> str:
    str_msg = ""
    for metric_type, metric_data in metrics_data.data.items():
        str_msg += f"\n>*{metric_type.value}:*"
        for metric_name, value in metric_data.metrics.items():
            whitespace = "          "

            if not value and value != 0:
                str_msg += f"\n>f{whitespace}- *{metric_name}:* No data found"
                continue

            prev_period_value = None
            prev_period_data: MetricsData = prev_period_metrics_data.data.get(
                metric_type, None
            )
            if prev_period_data:
                prev_period_value = prev_period_data.metrics.get(metric_name, None)
            print(
                f"[{metric_type.value}]Comparing metrics [{metric_name}]: ",
                value,
                prev_period_value,
            )

            compare_str = (
                "(No data for previous period)"
                if not prev_period_value and prev_period_value != 0
                else format_percentage_change(value, prev_period_value)
            )
            if metric_name == MetricsName.total_video_duration:
                value = seconds_to_readable_time(value)
            elif metric_name in [
                MetricsName.export_mp4_landscape_with_captions,
                MetricsName.export_mp4_landscape_without_captions,
                MetricsName.export_mp4_vertical_with_captions,
                MetricsName.export_mp4_vertical_without_captions,
            ]:
                whitespace *= 2

            str_msg += f"\n>{whitespace}- *{metric_name}:* {value} {compare_str}"
    return str_msg


def handle_daily_lambda_event():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    comparison_date = today - timedelta(days=2)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formated_date}) Eddie Daily Analytics"

    try:
        # Query data for daily report
        daily_metrics_data = get_analytics_metrics_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if not daily_metrics_data:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            # Query data for compare with daily report
            prev_period_metrics_data = get_analytics_metrics_data(
                comparison_date.strftime("%Y%m%d"), comparison_date.strftime("%Y%m%d")
            )
            print("Daily: ", daily_metrics_data)
            print("Compare with: ", prev_period_metrics_data)
            message = build_analytics_message(
                daily_metrics_data, prev_period_metrics_data
            )
    except Exception as e:
        print("Unexpected error occur in daily event --errors: ", e)
        message = f"> - *Error when preparing message for daily analytics*"
    print("Building slack message for daily eddie analytics...")
    return build_slack_template(header, message)


def handle_weekly_lambda_event():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    (
        monday_of_comparison_week,
        sunday_of_comparison_week,
    ) = get_start_end_of_week_by_offset(week_offset=2)

    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Weekly Analytics"

    try:
        weekly_metrics_data = get_analytics_metrics_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )

        if not weekly_metrics_data:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            prev_period_metrics_data = get_analytics_metrics_data(
                monday_of_comparison_week.strftime("%Y%m%d"),
                sunday_of_comparison_week.strftime("%Y%m%d"),
            )
            message = build_analytics_message(
                weekly_metrics_data, prev_period_metrics_data
            )

    except Exception as e:
        print("Unexpected error occur in weekly event --errors: ", e)
        message = f"> - *Error when preparing message for weekly analytics*"
    print("Building slack message for weekly eddie analytics...")
    return build_slack_template(header, message)


def get_analytics_metrics_data(start_date, end_date):
    # date params are in format str YYYYMMDD
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    project_creation_metrics = {}
    user_activity_metrics = {}
    share_acitivity_metrics = {}
    exports_metrics = {}

    # Project creation:
    # how many single cam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["singlecam"],
            }
        ],
    }

    singlecam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.singlecam_project_created: singlecam_project_created}
    )

    # how many multicam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["multicam"],
            }
        ],
    }
    multicam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.multicam_project_created: multicam_project_created}
    )

    # How many project created via web?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["browser"],
            }
        ],
    }

    project_created_by_web = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_web: project_created_by_web}
    )

    # How many project created via desktop?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["desktop"],
            }
        ],
    }

    project_created_by_desktop_app = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_desktop_app: project_created_by_desktop_app}
    )

    # User Activity:
    # How many new users?
    start_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    params = {
        "include_totals": "true",
        # auth0 require date in format YYYY-MM-DD
        "q": f"created_at:[{start_date_formatted} TO {end_date_formatted}]",
    }
    access_token = get_auth_token()
    response = search_auth0_users(params, access_token)
    new_user = response.get("total", 0)
    user_activity_metrics.update({MetricsName.new_user: new_user})

    # how many videos were uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_uploaded: file_were_uploaded})

    # how many hours of video uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
        "group_by": [
            {
                "type": "event",
                "value": "durationSec",
            }
        ],
    }
    total_video_duration = sum_filtered_amplitude_events(
        event_filters, start_date, end_date, AmplitudeQueryMetrics.sums, AMPLITUDE_AUTH
    )
    user_activity_metrics.update(
        {MetricsName.total_video_duration: total_video_duration}
    )

    # Unsuccessful Upload Attempts: (Unsupported language or no speech detected)
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_failed,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_upload_failed: file_were_uploaded})

    # how many returning users (users who already signed up who came back this week and created a new project)
    event_filters = {
        "event_type": AmplitudeEvents.returning_user,
    }
    returning_users = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.returning_user: returning_users})

    # Sharing Activity:
    # How many share links opened?
    event_filters = {
        "event_type": AmplitudeEvents.open_share_link,
    }
    share_links_opened = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.open_share_link: share_links_opened})

    # How many share links created?
    event_filters = {
        "event_type": AmplitudeEvents.create_share_link,
    }
    share_links_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.create_share_link: share_links_created})

    # Exports:
    # Total Export Mp4(Landscape)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions", "original"],
            },
        ],
    }

    total_export_lanscape = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_landscape: total_export_lanscape})

    # Export Mp4(Landscape) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions"],
            },
        ],
    }

    export_landscape_without_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {
            MetricsName.export_mp4_landscape_without_captions: export_landscape_without_captions
        }
    )

    # Export Mp4(Landscape) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original"],
            },
        ],
    }

    export_landscape_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_landscape_with_captions: export_landscape_with_captions}
    )

    # Total Export Mp4(Vertical)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions", "vertical"],
            },
        ],
    }

    total_export_vertical = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_vertical: total_export_vertical})

    # Export Mp4(Vertical) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions"],
            },
        ],
    }

    vertical_no_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_without_captions: vertical_no_captions}
    )

    # Export Mp4(Vertical) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical"],
            },
        ],
    }

    vertical_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_with_captions: vertical_with_captions}
    )

    # how many exports adobe premiere
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["premiere"],
            }
        ],
    }

    export_premiere = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_adobe: export_premiere})

    # how many exports davinci resolve
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["davinci"],
            }
        ],
    }

    export_davinci = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_resolve: export_davinci})

    # how many exports to fcp
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["fcpxml"],
            }
        ],
    }
    export_fcp = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_fcp: export_fcp})

    # how many exports (total)
    event_filters = {"event_type": AmplitudeEvents.user_click_export}
    total_exports = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.total_exports: total_exports})

    return ReportMetrics(
        data={
            MetricCategory.project_creation: MetricsData(
                metrics=project_creation_metrics
            ),
            MetricCategory.user_activity: MetricsData(metrics=user_activity_metrics),
            MetricCategory.share_activity: MetricsData(metrics=share_acitivity_metrics),
            MetricCategory.export: MetricsData(metrics=exports_metrics),
        }
    )


def parse_export_data(extracted_file_paths: List[str]) -> List[UserActivity]:
    # Summarize user activity
    user_store = UserActivityStore()

    for json_file in extracted_file_paths:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            for index, event in enumerate(data, start=1):
                if event.get("data_type") != "event":
                    continue

                user_id = (
                    event.get("user_id")
                    or f'Anonymous (Amplitude ID-{event.get("amplitude_id", index)})'
                )
                user_activity = user_store.get_activity(user_id)
                project = handle_export_event(event, user_activity)
                if project:
                    user_activity.add_project(project)
                    user_store.add_activity(user_activity)
    return user_store.get_all_activities()


def handle_export_event(event, user_activity: UserActivity):
    event_type = event.get("event_type")
    event_properties = event.get("event_properties")
    project_id = event_properties.get("projectId")
    project_name = event_properties.get("projectName")

    try:
        raw_files_info = event_properties.get("projectFileInfo", "[]")
        files_info = (
            json.loads(raw_files_info)
            if isinstance(raw_files_info, str)
            else raw_files_info
        )

        project_files_info = (
            ", ".join(
                [
                    f"{file.get('filename')} ({format_bytes(int(file.get('filesize', 0)))})"
                    for file in files_info
                ]
            )
            if files_info and isinstance(files_info, list)
            else None
        )
    except (json.JSONDecodeError, TypeError) as e:
        project_files_info = None
        print(f"Error parsing projectFileInfo: {e}")

    if event_type in {
        AmplitudeEvents.user_submit_prompt,
        AmplitudeEvents.user_click_prompt_suggest,
    }:
        prompt = (
            event_properties.get("prompt")
            if event_type == AmplitudeEvents.user_submit_prompt
            else event_properties.get("promptSuggest")
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_prompt(prompt)
        else:
            # Init new project with prompt
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[prompt],
                exports=[],
            )
        return project

    elif event_type == AmplitudeEvents.file_export_succeeded:
        layout = event_properties.get("layout")
        export_type = event_properties.get("exportType")
        ouput_duration_str = event_properties.get("outputDuration", None)
        readable_duration = (
            seconds_to_readable_time(float(ouput_duration_str))
            if ouput_duration_str
            else None
        )
        export_filename = event_properties.get("exportFileName")
        export_info = ExportInfo(
            export_layout=layout,
            export_type=export_type,
            export_file_name=export_filename,
            output_duration=readable_duration,
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_export(export_info)
        else:
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[],
                exports=[export_info],
            )
        return project
    else:
        return None


def handle_export_data_response(export_data) -> List[str]:
    os.makedirs(os.path.dirname(EXTRACTED_DATA_FILEPATH), exist_ok=True)
    # Write export data as zip
    with open(f"{EXPORT_FILEPATH}.zip", "wb") as f:
        f.write(export_data.content)
    # Extract data from zip archive
    with zipfile.ZipFile(f"{EXPORT_FILEPATH}.zip", "r") as zip_ref:
        zip_ref.extractall(f"/tmp/{EXPORT_FILENAME}")

    return os.listdir(EXTRACTED_DATA_FILEPATH)


def extract_export_data(files: List[str]) -> List[str]:
    ouput_file_paths = []

    # Extract export data to json
    for file in files:
        if file.endswith(".json.gz"):
            file_path = os.path.join(EXTRACTED_DATA_FILEPATH, file)

            extracted_data = []
            with gzip.open(file_path, "rt", encoding="utf-8") as gz_file:
                for line in gz_file:
                    json_obj = json.loads(line.strip())
                    extracted_data.append(json_obj)

            output_file = file.replace(".gz", "")
            output_path = os.path.join(EXTRACTED_DATA_FILEPATH, output_file)

            with open(output_path, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file, indent=4)

            ouput_file_paths.append(EXTRACTED_DATA_FILEPATH + output_file)
            print(f"Extracted: {output_file}")
    return ouput_file_paths


async def handle_send_daily_users(
    date: str, user_store: List[UserActivity]
) -> Dict[str, str]:
    # Send report header
    header = f"🗓️ ({date}) Daily User Interactions\n"

    await send_slack_in_thread_message(
        {
            "blocks": [
                HeaderBlock(text=f"{header}").to_dict(),
                SectionBlock(text=" ").to_dict(),
            ]
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )

    users_thread = {}
    # Send user email message and init thread
    for idx, user_activity in enumerate(user_store, start=1):
        message = f"{idx}) 👤User: {user_activity.email}\n"
        # chat.postMessage has special rate limiting conditions (1msg/s)
        # https://api.slack.com/methods/chat.postMessage#rate_limiting
        await asyncio.sleep(1)
        res = await send_slack_in_thread_message(
            {"text": message}, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        users_thread[user_activity.email] = res.get("ts")

    # Send report footer
    await send_slack_in_thread_message(
        {
            "blocks": [
                SectionBlock(
                    text=f":checkered_flag: *End of Report:* {header}"
                ).to_dict(),
                SectionBlock(text=" ").to_dict(),
                DividerBlock(type="divider").to_dict(),
            ],
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )
    return users_thread


async def handle_send_message_by_thread(
    users_thread: Dict[str, str], user_store: List[UserActivity]
):
    try:
        whitespace = "        "

        for idx, user_activity in enumerate(user_store, start=1):
            parsed_str = []

            for project_idx, project in enumerate(
                user_activity.projects.values(), start=1
            ):
                project_line = f":movie_camera: #{project_idx}. *Project name:* {project.project_name}\n"
                parsed_str.append(f"{project_line}")

                if project.file_info:
                    parsed_str.append(
                        f"{whitespace*2}- *File info:* {project.file_info}\n"
                    )

                if len(project.prompts) > 0:
                    parsed_str.append(f"{whitespace}:man-raising-hand: *Prompts:*\n")
                    for prompt_idx, prompt in enumerate(project.prompts, start=1):
                        parsed_str.append(f"{whitespace*2} - #{prompt_idx}: {prompt}\n")

                if len(project.exports) > 0:
                    parsed_str.append(f"{whitespace}:inbox_tray: *Exports:*\n")
                    for export_idx, export in enumerate(project.exports, start=1):
                        export_msg = (
                            f"{whitespace*2} - #{export_idx}:  Format: {export.export_layout}-{export.export_type}"
                            if export.export_layout
                            else f"{whitespace*2} - #{export_idx}:  Format: {export.export_type}"
                        )
                        if export.output_duration:
                            export_msg = (
                                f"{export_msg}, duration: {export.output_duration}"
                            )
                        parsed_str.append(f"{export_msg}\n")

            message_str = "".join(parsed_str)
            i = 0
            while i < len(message_str):
                end_index = min(i + SLACK_MESSAGE_LIMIT, len(message_str))
                if end_index < len(message_str) and "\n" in message_str[i:end_index]:
                    end_index = message_str.rfind("\n", i, end_index) + 1

                message = message_str[i:end_index]
                # chat.postMessage has special rate limiting conditions (1msg/s)
                # https://api.slack.com/methods/chat.postMessage#rate_limiting
                await asyncio.sleep(1)
                await send_slack_in_thread_message(
                    message={"text": message},
                    channel_id=EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
                    thread_ts=users_thread[user_activity.email],
                )
                i = end_index

    except Exception as e:
        print(f"Error handle_send_message_by_thread --errors: '{e}'")
        raise


async def handle_summarizes_user_interactions():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.strftime("%Y%m%d") + "T00"
    end_date = yesterday.strftime("%Y%m%d") + "T23"
    # format to readable date for send slack message
    formatted_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_date}) Daily User Interactions\n"

    export_data = export_amplitude_events(start_date, end_date)
    if not export_data:
        message = f"> - *Error when exporting events for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        return

    try:
        ouptut_file_paths = handle_export_data_response(export_data)
        extracted_file_paths = extract_export_data(ouptut_file_paths)
        print(
            "Exported data, preparing data for daily summarizes user interactions analytics..."
        )
        data = parse_export_data(extracted_file_paths)
        print("Sending slack messsage...")
        if len(data) == 0:
            message = f"> - *Export data not found*"
            slack_template = build_slack_template(header, message)
            await send_slack_in_thread_message(
                slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
            )
        else:
            users_thread = await handle_send_daily_users(formatted_date, data)
            await handle_send_message_by_thread(users_thread, data)
    except Exception as e:
        print(
            "Unexpected error occur in daily summarizes user interactions event --errors: ",
            e,
        )
        message = f"> - *Error when preparing data for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
    finally:
        print("Sent slack message for daily summarizes user interactions analytics")
        free_up_disk_space(f"{EXPORT_FILEPATH}.zip")
        free_up_disk_space(EXTRACTED_DATA_FILEPATH)


def get_prompting_time_data(start_date, end_date) -> tuple:
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    # Average time taken for first eddie response for all users (Avarage the first time time taken when a new user prompts.)
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "filters": [
            {
                "subprop_type": "nth_time_hack",
                "subprop_key": "nth_time_performed",
                "subprop_op": "is",
                "subprop_value": ["1"],
            }
        ],
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_first_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )

    # Average time taken for all eddie responses
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_all_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )
    return avg_first_res_in_ms, avg_all_res_in_ms


def build_prompting_time_message(avg_first_res_in_ms, avg_all_res_in_ms):
    white_space = "            "
    message_str = "> 📝 *Average time taken for*:\n"
    message_str += f">{white_space}- *All eddie responses*: {seconds_to_readable_time(avg_all_res_in_ms/1000)}\n"
    message_str += f">{white_space}- *First eddie response for new users*: {seconds_to_readable_time(avg_first_res_in_ms/1000)}\n"
    return message_str


def handle_daily_prompting_time_report():
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"⏰ ({formated_date}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            print(
                f"Daily report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing daily prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing daily prompting time report*"
    print("Building slack message for daily prompting time report...")
    return build_slack_template(header, message)


def handle_weekly_prompting_time_report():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"⏰ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            print(
                f"Weekly report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing weekly prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing weekly prompting time report*"
    print("Building slack message for weekly prompting time report...")
    return build_slack_template(header, message)
import asyncio
import base64
import gzip
import json
import os
import zipfile

from datetime import datetime, timedelta
from typing import Dict, List
from config import (
    AMPLITUDE_API_KEY,
    AMPLITUDE_SECRET_KEY,
    AMPLITUDE_PROJECT_ID,
    EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
)
from models.export_model import ExportInfo
from models.metrics_model import MetricsData, ReportMetrics
from slack_sdk.models.blocks import SectionBlock, HeaderBlock, DividerBlock
from services.auth0.auth0_service import get_auth_token, search_auth0_users
from models.project_model import ProjectInfo
from models.user_activity_model import UserActivity, UserActivityStore
from services.slack.send_message import send_slack_in_thread_message, send_slack_message
from utils import get_start_end_of_week_by_offset
from services.slack.message_templates import build_slack_template
from utils import (
    format_bytes,
    free_up_disk_space,
    format_percentage_change,
    seconds_to_readable_time,
)
from enums import AmplitudeEvents, AmplitudeQueryMetrics, MetricCategory, MetricsName
from services.amplitude.amplitude_service import (
    export_amplitude_events,
    sum_filtered_amplitude_events,
)

SLACK_MESSAGE_LIMIT = 3000
EXPORT_FILENAME = "export_data"
# Save in /tmp to make it writable in AWS Lambda.
# https://repost.aws/questions/QUyYQzTTPnRY6_2w71qscojA/read-only-file-system-aws-lambda
EXPORT_FILEPATH = f"/tmp/{EXPORT_FILENAME}"
EXTRACTED_DATA_FILEPATH = f"/tmp/{EXPORT_FILENAME}/{AMPLITUDE_PROJECT_ID}/"


def build_analytics_message(
    metrics_data: ReportMetrics, prev_period_metrics_data: ReportMetrics
) -> str:
    str_msg = ""
    for metric_type, metric_data in metrics_data.data.items():
        str_msg += f"\n>*{metric_type.value}:*"
        for metric_name, value in metric_data.metrics.items():
            whitespace = "          "

            if not value and value != 0:
                str_msg += f"\n>f{whitespace}- *{metric_name}:* No data found"
                continue

            prev_period_value = None
            prev_period_data: MetricsData = prev_period_metrics_data.data.get(
                metric_type, None
            )
            if prev_period_data:
                prev_period_value = prev_period_data.metrics.get(metric_name, None)
            print(
                f"[{metric_type.value}]Comparing metrics [{metric_name}]: ",
                value,
                prev_period_value,
            )

            compare_str = (
                "(No data for previous period)"
                if not prev_period_value and prev_period_value != 0
                else format_percentage_change(value, prev_period_value)
            )
            if metric_name == MetricsName.total_video_duration:
                value = seconds_to_readable_time(value)
            elif metric_name in [
                MetricsName.export_mp4_landscape_with_captions,
                MetricsName.export_mp4_landscape_without_captions,
                MetricsName.export_mp4_vertical_with_captions,
                MetricsName.export_mp4_vertical_without_captions,
            ]:
                whitespace *= 2

            str_msg += f"\n>{whitespace}- *{metric_name}:* {value} {compare_str}"
    return str_msg


def handle_daily_lambda_event():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    comparison_date = today - timedelta(days=2)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formated_date}) Eddie Daily Analytics"

    try:
        # Query data for daily report
        daily_metrics_data = get_analytics_metrics_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if not daily_metrics_data:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            # Query data for compare with daily report
            prev_period_metrics_data = get_analytics_metrics_data(
                comparison_date.strftime("%Y%m%d"), comparison_date.strftime("%Y%m%d")
            )
            print("Daily: ", daily_metrics_data)
            print("Compare with: ", prev_period_metrics_data)
            message = build_analytics_message(
                daily_metrics_data, prev_period_metrics_data
            )
    except Exception as e:
        print("Unexpected error occur in daily event --errors: ", e)
        message = f"> - *Error when preparing message for daily analytics*"
    print("Building slack message for daily eddie analytics...")
    return build_slack_template(header, message)


def handle_weekly_lambda_event():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    (
        monday_of_comparison_week,
        sunday_of_comparison_week,
    ) = get_start_end_of_week_by_offset(week_offset=2)

    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Weekly Analytics"

    try:
        weekly_metrics_data = get_analytics_metrics_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )

        if not weekly_metrics_data:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            prev_period_metrics_data = get_analytics_metrics_data(
                monday_of_comparison_week.strftime("%Y%m%d"),
                sunday_of_comparison_week.strftime("%Y%m%d"),
            )
            message = build_analytics_message(
                weekly_metrics_data, prev_period_metrics_data
            )

    except Exception as e:
        print("Unexpected error occur in weekly event --errors: ", e)
        message = f"> - *Error when preparing message for weekly analytics*"
    print("Building slack message for weekly eddie analytics...")
    return build_slack_template(header, message)


def get_analytics_metrics_data(start_date, end_date):
    # date params are in format str YYYYMMDD
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    project_creation_metrics = {}
    user_activity_metrics = {}
    share_acitivity_metrics = {}
    exports_metrics = {}

    # Project creation:
    # how many single cam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["singlecam"],
            }
        ],
    }

    singlecam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.singlecam_project_created: singlecam_project_created}
    )

    # how many multicam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["multicam"],
            }
        ],
    }
    multicam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.multicam_project_created: multicam_project_created}
    )

    # How many project created via web?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["browser"],
            }
        ],
    }

    project_created_by_web = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_web: project_created_by_web}
    )

    # How many project created via desktop?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["desktop"],
            }
        ],
    }

    project_created_by_desktop_app = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_desktop_app: project_created_by_desktop_app}
    )

    # User Activity:
    # How many new users?
    start_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    params = {
        "include_totals": "true",
        # auth0 require date in format YYYY-MM-DD
        "q": f"created_at:[{start_date_formatted} TO {end_date_formatted}]",
    }
    access_token = get_auth_token()
    response = search_auth0_users(params, access_token)
    new_user = response.get("total", 0)
    user_activity_metrics.update({MetricsName.new_user: new_user})

    # how many videos were uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_uploaded: file_were_uploaded})

    # how many hours of video uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
        "group_by": [
            {
                "type": "event",
                "value": "durationSec",
            }
        ],
    }
    total_video_duration = sum_filtered_amplitude_events(
        event_filters, start_date, end_date, AmplitudeQueryMetrics.sums, AMPLITUDE_AUTH
    )
    user_activity_metrics.update(
        {MetricsName.total_video_duration: total_video_duration}
    )

    # Unsuccessful Upload Attempts: (Unsupported language or no speech detected)
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_failed,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_upload_failed: file_were_uploaded})

    # how many returning users (users who already signed up who came back this week and created a new project)
    event_filters = {
        "event_type": AmplitudeEvents.returning_user,
    }
    returning_users = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.returning_user: returning_users})

    # Sharing Activity:
    # How many share links opened?
    event_filters = {
        "event_type": AmplitudeEvents.open_share_link,
    }
    share_links_opened = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.open_share_link: share_links_opened})

    # How many share links created?
    event_filters = {
        "event_type": AmplitudeEvents.create_share_link,
    }
    share_links_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.create_share_link: share_links_created})

    # Exports:
    # Total Export Mp4(Landscape)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions", "original"],
            },
        ],
    }

    total_export_lanscape = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_landscape: total_export_lanscape})

    # Export Mp4(Landscape) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions"],
            },
        ],
    }

    export_landscape_without_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {
            MetricsName.export_mp4_landscape_without_captions: export_landscape_without_captions
        }
    )

    # Export Mp4(Landscape) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original"],
            },
        ],
    }

    export_landscape_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_landscape_with_captions: export_landscape_with_captions}
    )

    # Total Export Mp4(Vertical)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions", "vertical"],
            },
        ],
    }

    total_export_vertical = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_vertical: total_export_vertical})

    # Export Mp4(Vertical) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions"],
            },
        ],
    }

    vertical_no_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_without_captions: vertical_no_captions}
    )

    # Export Mp4(Vertical) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical"],
            },
        ],
    }

    vertical_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_with_captions: vertical_with_captions}
    )

    # how many exports adobe premiere
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["premiere"],
            }
        ],
    }

    export_premiere = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_adobe: export_premiere})

    # how many exports davinci resolve
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["davinci"],
            }
        ],
    }

    export_davinci = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_resolve: export_davinci})

    # how many exports to fcp
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["fcpxml"],
            }
        ],
    }
    export_fcp = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_fcp: export_fcp})

    # how many exports (total)
    event_filters = {"event_type": AmplitudeEvents.user_click_export}
    total_exports = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.total_exports: total_exports})

    return ReportMetrics(
        data={
            MetricCategory.project_creation: MetricsData(
                metrics=project_creation_metrics
            ),
            MetricCategory.user_activity: MetricsData(metrics=user_activity_metrics),
            MetricCategory.share_activity: MetricsData(metrics=share_acitivity_metrics),
            MetricCategory.export: MetricsData(metrics=exports_metrics),
        }
    )


def parse_export_data(extracted_file_paths: List[str]) -> List[UserActivity]:
    # Summarize user activity
    user_store = UserActivityStore()

    for json_file in extracted_file_paths:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            for index, event in enumerate(data, start=1):
                if event.get("data_type") != "event":
                    continue

                user_id = (
                    event.get("user_id")
                    or f'Anonymous (Amplitude ID-{event.get("amplitude_id", index)})'
                )
                user_activity = user_store.get_activity(user_id)
                project = handle_export_event(event, user_activity)
                if project:
                    user_activity.add_project(project)
                    user_store.add_activity(user_activity)
    return user_store.get_all_activities()


def handle_export_event(event, user_activity: UserActivity):
    event_type = event.get("event_type")
    event_properties = event.get("event_properties")
    project_id = event_properties.get("projectId")
    project_name = event_properties.get("projectName")

    try:
        raw_files_info = event_properties.get("projectFileInfo", "[]")
        files_info = (
            json.loads(raw_files_info)
            if isinstance(raw_files_info, str)
            else raw_files_info
        )

        project_files_info = (
            ", ".join(
                [
                    f"{file.get('filename')} ({format_bytes(int(file.get('filesize', 0)))})"
                    for file in files_info
                ]
            )
            if files_info and isinstance(files_info, list)
            else None
        )
    except (json.JSONDecodeError, TypeError) as e:
        project_files_info = None
        print(f"Error parsing projectFileInfo: {e}")

    if event_type in {
        AmplitudeEvents.user_submit_prompt,
        AmplitudeEvents.user_click_prompt_suggest,
    }:
        prompt = (
            event_properties.get("prompt")
            if event_type == AmplitudeEvents.user_submit_prompt
            else event_properties.get("promptSuggest")
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_prompt(prompt)
        else:
            # Init new project with prompt
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[prompt],
                exports=[],
            )
        return project

    elif event_type == AmplitudeEvents.file_export_succeeded:
        layout = event_properties.get("layout")
        export_type = event_properties.get("exportType")
        ouput_duration_str = event_properties.get("outputDuration", None)
        readable_duration = (
            seconds_to_readable_time(float(ouput_duration_str))
            if ouput_duration_str
            else None
        )
        export_filename = event_properties.get("exportFileName")
        export_info = ExportInfo(
            export_layout=layout,
            export_type=export_type,
            export_file_name=export_filename,
            output_duration=readable_duration,
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_export(export_info)
        else:
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[],
                exports=[export_info],
            )
        return project
    else:
        return None


def handle_export_data_response(export_data) -> List[str]:
    os.makedirs(os.path.dirname(EXTRACTED_DATA_FILEPATH), exist_ok=True)
    # Write export data as zip
    with open(f"{EXPORT_FILEPATH}.zip", "wb") as f:
        f.write(export_data.content)
    # Extract data from zip archive
    with zipfile.ZipFile(f"{EXPORT_FILEPATH}.zip", "r") as zip_ref:
        zip_ref.extractall(f"/tmp/{EXPORT_FILENAME}")

    return os.listdir(EXTRACTED_DATA_FILEPATH)


def extract_export_data(files: List[str]) -> List[str]:
    ouput_file_paths = []

    # Extract export data to json
    for file in files:
        if file.endswith(".json.gz"):
            file_path = os.path.join(EXTRACTED_DATA_FILEPATH, file)

            extracted_data = []
            with gzip.open(file_path, "rt", encoding="utf-8") as gz_file:
                for line in gz_file:
                    json_obj = json.loads(line.strip())
                    extracted_data.append(json_obj)

            output_file = file.replace(".gz", "")
            output_path = os.path.join(EXTRACTED_DATA_FILEPATH, output_file)

            with open(output_path, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file, indent=4)

            ouput_file_paths.append(EXTRACTED_DATA_FILEPATH + output_file)
            print(f"Extracted: {output_file}")
    return ouput_file_paths


async def handle_send_daily_users(
    date: str, user_store: List[UserActivity]
) -> Dict[str, str]:
    # Send report header
    header = f"🗓️ ({date}) Daily User Interactions\n"

    await send_slack_in_thread_message(
        {
            "blocks": [
                HeaderBlock(text=f"{header}").to_dict(),
                SectionBlock(text=" ").to_dict(),
            ]
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )

    users_thread = {}
    # Send user email message and init thread
    for idx, user_activity in enumerate(user_store, start=1):
        message = f"{idx}) 👤User: {user_activity.email}\n"
        # chat.postMessage has special rate limiting conditions (1msg/s)
        # https://api.slack.com/methods/chat.postMessage#rate_limiting
        await asyncio.sleep(1)
        res = await send_slack_in_thread_message(
            {"text": message}, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        users_thread[user_activity.email] = res.get("ts")

    # Send report footer
    await send_slack_in_thread_message(
        {
            "blocks": [
                SectionBlock(
                    text=f":checkered_flag: *End of Report:* {header}"
                ).to_dict(),
                SectionBlock(text=" ").to_dict(),
                DividerBlock(type="divider").to_dict(),
            ],
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )
    return users_thread


async def handle_send_message_by_thread(
    users_thread: Dict[str, str], user_store: List[UserActivity]
):
    try:
        whitespace = "        "

        for idx, user_activity in enumerate(user_store, start=1):
            parsed_str = []

            for project_idx, project in enumerate(
                user_activity.projects.values(), start=1
            ):
                project_line = f":movie_camera: #{project_idx}. *Project name:* {project.project_name}\n"
                parsed_str.append(f"{project_line}")

                if project.file_info:
                    parsed_str.append(
                        f"{whitespace*2}- *File info:* {project.file_info}\n"
                    )

                if len(project.prompts) > 0:
                    parsed_str.append(f"{whitespace}:man-raising-hand: *Prompts:*\n")
                    for prompt_idx, prompt in enumerate(project.prompts, start=1):
                        parsed_str.append(f"{whitespace*2} - #{prompt_idx}: {prompt}\n")

                if len(project.exports) > 0:
                    parsed_str.append(f"{whitespace}:inbox_tray: *Exports:*\n")
                    for export_idx, export in enumerate(project.exports, start=1):
                        export_msg = (
                            f"{whitespace*2} - #{export_idx}:  Format: {export.export_layout}-{export.export_type}"
                            if export.export_layout
                            else f"{whitespace*2} - #{export_idx}:  Format: {export.export_type}"
                        )
                        if export.output_duration:
                            export_msg = (
                                f"{export_msg}, duration: {export.output_duration}"
                            )
                        parsed_str.append(f"{export_msg}\n")

            message_str = "".join(parsed_str)
            i = 0
            while i < len(message_str):
                end_index = min(i + SLACK_MESSAGE_LIMIT, len(message_str))
                if end_index < len(message_str) and "\n" in message_str[i:end_index]:
                    end_index = message_str.rfind("\n", i, end_index) + 1

                message = message_str[i:end_index]
                # chat.postMessage has special rate limiting conditions (1msg/s)
                # https://api.slack.com/methods/chat.postMessage#rate_limiting
                await asyncio.sleep(1)
                await send_slack_in_thread_message(
                    message={"text": message},
                    channel_id=EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
                    thread_ts=users_thread[user_activity.email],
                )
                i = end_index

    except Exception as e:
        print(f"Error handle_send_message_by_thread --errors: '{e}'")
        raise


async def handle_summarizes_user_interactions():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.strftime("%Y%m%d") + "T00"
    end_date = yesterday.strftime("%Y%m%d") + "T23"
    # format to readable date for send slack message
    formatted_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_date}) Daily User Interactions\n"

    export_data = export_amplitude_events(start_date, end_date)
    if not export_data:
        message = f"> - *Error when exporting events for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        return

    try:
        ouptut_file_paths = handle_export_data_response(export_data)
        extracted_file_paths = extract_export_data(ouptut_file_paths)
        print(
            "Exported data, preparing data for daily summarizes user interactions analytics..."
        )
        data = parse_export_data(extracted_file_paths)
        print("Sending slack messsage...")
        if len(data) == 0:
            message = f"> - *Export data not found*"
            slack_template = build_slack_template(header, message)
            await send_slack_in_thread_message(
                slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
            )
        else:
            users_thread = await handle_send_daily_users(formatted_date, data)
            await handle_send_message_by_thread(users_thread, data)
    except Exception as e:
        print(
            "Unexpected error occur in daily summarizes user interactions event --errors: ",
            e,
        )
        message = f"> - *Error when preparing data for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
    finally:
        print("Sent slack message for daily summarizes user interactions analytics")
        free_up_disk_space(f"{EXPORT_FILEPATH}.zip")
        free_up_disk_space(EXTRACTED_DATA_FILEPATH)


def get_prompting_time_data(start_date, end_date) -> tuple:
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    # Average time taken for first eddie response for all users (Avarage the first time time taken when a new user prompts.)
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "filters": [
            {
                "subprop_type": "nth_time_hack",
                "subprop_key": "nth_time_performed",
                "subprop_op": "is",
                "subprop_value": ["1"],
            }
        ],
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_first_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )

    # Average time taken for all eddie responses
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_all_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )
    return avg_first_res_in_ms, avg_all_res_in_ms


def build_prompting_time_message(avg_first_res_in_ms, avg_all_res_in_ms):
    white_space = "            "
    message_str = "> 📝 *Average time taken for*:\n"
    message_str += f">{white_space}- *All eddie responses*: {seconds_to_readable_time(avg_all_res_in_ms/1000)}\n"
    message_str += f">{white_space}- *First eddie response for new users*: {seconds_to_readable_time(avg_first_res_in_ms/1000)}\n"
    return message_str


def handle_daily_prompting_time_report():
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"⏰ ({formated_date}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            print(
                f"Daily report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing daily prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing daily prompting time report*"
    print("Building slack message for daily prompting time report...")
    return build_slack_template(header, message)


def handle_weekly_prompting_time_report():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"⏰ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            print(
                f"Weekly report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing weekly prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing weekly prompting time report*"
    print("Building slack message for weekly prompting time report...")
    return build_slack_template(header, message)
import asyncio
import base64
import gzip
import json
import os
import zipfile

from datetime import datetime, timedelta
from typing import Dict, List
from config import (
    AMPLITUDE_API_KEY,
    AMPLITUDE_SECRET_KEY,
    AMPLITUDE_PROJECT_ID,
    EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
)
from models.export_model import ExportInfo
from models.metrics_model import MetricsData, ReportMetrics
from slack_sdk.models.blocks import SectionBlock, HeaderBlock, DividerBlock
from services.auth0.auth0_service import get_auth_token, search_auth0_users
from models.project_model import ProjectInfo
from models.user_activity_model import UserActivity, UserActivityStore
from services.slack.send_message import send_slack_in_thread_message, send_slack_message
from utils import get_start_end_of_week_by_offset
from services.slack.message_templates import build_slack_template
from utils import (
    format_bytes,
    free_up_disk_space,
    format_percentage_change,
    seconds_to_readable_time,
)
from enums import AmplitudeEvents, AmplitudeQueryMetrics, MetricCategory, MetricsName
from services.amplitude.amplitude_service import (
    export_amplitude_events,
    sum_filtered_amplitude_events,
)

SLACK_MESSAGE_LIMIT = 3000
EXPORT_FILENAME = "export_data"
# Save in /tmp to make it writable in AWS Lambda.
# https://repost.aws/questions/QUyYQzTTPnRY6_2w71qscojA/read-only-file-system-aws-lambda
EXPORT_FILEPATH = f"/tmp/{EXPORT_FILENAME}"
EXTRACTED_DATA_FILEPATH = f"/tmp/{EXPORT_FILENAME}/{AMPLITUDE_PROJECT_ID}/"


def build_analytics_message(
    metrics_data: ReportMetrics, prev_period_metrics_data: ReportMetrics
) -> str:
    str_msg = ""
    for metric_type, metric_data in metrics_data.data.items():
        str_msg += f"\n>*{metric_type.value}:*"
        for metric_name, value in metric_data.metrics.items():
            whitespace = "          "

            if not value and value != 0:
                str_msg += f"\n>f{whitespace}- *{metric_name}:* No data found"
                continue

            prev_period_value = None
            prev_period_data: MetricsData = prev_period_metrics_data.data.get(
                metric_type, None
            )
            if prev_period_data:
                prev_period_value = prev_period_data.metrics.get(metric_name, None)
            print(
                f"[{metric_type.value}]Comparing metrics [{metric_name}]: ",
                value,
                prev_period_value,
            )

            compare_str = (
                "(No data for previous period)"
                if not prev_period_value and prev_period_value != 0
                else format_percentage_change(value, prev_period_value)
            )
            if metric_name == MetricsName.total_video_duration:
                value = seconds_to_readable_time(value)
            elif metric_name in [
                MetricsName.export_mp4_landscape_with_captions,
                MetricsName.export_mp4_landscape_without_captions,
                MetricsName.export_mp4_vertical_with_captions,
                MetricsName.export_mp4_vertical_without_captions,
            ]:
                whitespace *= 2

            str_msg += f"\n>{whitespace}- *{metric_name}:* {value} {compare_str}"
    return str_msg


def handle_daily_lambda_event():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    comparison_date = today - timedelta(days=2)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formated_date}) Eddie Daily Analytics"

    try:
        # Query data for daily report
        daily_metrics_data = get_analytics_metrics_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if not daily_metrics_data:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            # Query data for compare with daily report
            prev_period_metrics_data = get_analytics_metrics_data(
                comparison_date.strftime("%Y%m%d"), comparison_date.strftime("%Y%m%d")
            )
            print("Daily: ", daily_metrics_data)
            print("Compare with: ", prev_period_metrics_data)
            message = build_analytics_message(
                daily_metrics_data, prev_period_metrics_data
            )
    except Exception as e:
        print("Unexpected error occur in daily event --errors: ", e)
        message = f"> - *Error when preparing message for daily analytics*"
    print("Building slack message for daily eddie analytics...")
    return build_slack_template(header, message)


def handle_weekly_lambda_event():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    (
        monday_of_comparison_week,
        sunday_of_comparison_week,
    ) = get_start_end_of_week_by_offset(week_offset=2)

    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Weekly Analytics"

    try:
        weekly_metrics_data = get_analytics_metrics_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )

        if not weekly_metrics_data:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            prev_period_metrics_data = get_analytics_metrics_data(
                monday_of_comparison_week.strftime("%Y%m%d"),
                sunday_of_comparison_week.strftime("%Y%m%d"),
            )
            message = build_analytics_message(
                weekly_metrics_data, prev_period_metrics_data
            )

    except Exception as e:
        print("Unexpected error occur in weekly event --errors: ", e)
        message = f"> - *Error when preparing message for weekly analytics*"
    print("Building slack message for weekly eddie analytics...")
    return build_slack_template(header, message)


def get_analytics_metrics_data(start_date, end_date):
    # date params are in format str YYYYMMDD
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    project_creation_metrics = {}
    user_activity_metrics = {}
    share_acitivity_metrics = {}
    exports_metrics = {}

    # Project creation:
    # how many single cam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["singlecam"],
            }
        ],
    }

    singlecam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.singlecam_project_created: singlecam_project_created}
    )

    # how many multicam projects created?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "projectCategory",
                "subprop_op": "is",
                "subprop_value": ["multicam"],
            }
        ],
    }
    multicam_project_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.multicam_project_created: multicam_project_created}
    )

    # How many project created via web?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["browser"],
            }
        ],
    }

    project_created_by_web = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_web: project_created_by_web}
    )

    # How many project created via desktop?
    event_filters = {
        "event_type": AmplitudeEvents.project_import_succeeded,
        "filters": [
            {
                "subprop_type": "user",
                "subprop_key": "platform",
                "subprop_op": "is",
                "subprop_value": ["desktop"],
            }
        ],
    }

    project_created_by_desktop_app = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    project_creation_metrics.update(
        {MetricsName.project_created_by_desktop_app: project_created_by_desktop_app}
    )

    # User Activity:
    # How many new users?
    start_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    params = {
        "include_totals": "true",
        # auth0 require date in format YYYY-MM-DD
        "q": f"created_at:[{start_date_formatted} TO {end_date_formatted}]",
    }
    access_token = get_auth_token()
    response = search_auth0_users(params, access_token)
    new_user = response.get("total", 0)
    user_activity_metrics.update({MetricsName.new_user: new_user})

    # how many videos were uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_uploaded: file_were_uploaded})

    # how many hours of video uploaded
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_succeeded,
        "group_by": [
            {
                "type": "event",
                "value": "durationSec",
            }
        ],
    }
    total_video_duration = sum_filtered_amplitude_events(
        event_filters, start_date, end_date, AmplitudeQueryMetrics.sums, AMPLITUDE_AUTH
    )
    user_activity_metrics.update(
        {MetricsName.total_video_duration: total_video_duration}
    )

    # Unsuccessful Upload Attempts: (Unsupported language or no speech detected)
    event_filters = {
        "event_type": AmplitudeEvents.file_upload_failed,
    }
    file_were_uploaded = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.file_upload_failed: file_were_uploaded})

    # how many returning users (users who already signed up who came back this week and created a new project)
    event_filters = {
        "event_type": AmplitudeEvents.returning_user,
    }
    returning_users = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    user_activity_metrics.update({MetricsName.returning_user: returning_users})

    # Sharing Activity:
    # How many share links opened?
    event_filters = {
        "event_type": AmplitudeEvents.open_share_link,
    }
    share_links_opened = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.open_share_link: share_links_opened})

    # How many share links created?
    event_filters = {
        "event_type": AmplitudeEvents.create_share_link,
    }
    share_links_created = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    share_acitivity_metrics.update({MetricsName.create_share_link: share_links_created})

    # Exports:
    # Total Export Mp4(Landscape)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions", "original"],
            },
        ],
    }

    total_export_lanscape = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_landscape: total_export_lanscape})

    # Export Mp4(Landscape) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original_no_captions"],
            },
        ],
    }

    export_landscape_without_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {
            MetricsName.export_mp4_landscape_without_captions: export_landscape_without_captions
        }
    )

    # Export Mp4(Landscape) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["original"],
            },
        ],
    }

    export_landscape_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_landscape_with_captions: export_landscape_with_captions}
    )

    # Total Export Mp4(Vertical)
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions", "vertical"],
            },
        ],
    }

    total_export_vertical = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_mp4_vertical: total_export_vertical})

    # Export Mp4(Vertical) Without captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical_no_captions"],
            },
        ],
    }

    vertical_no_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_without_captions: vertical_no_captions}
    )

    # Export Mp4(Vertical) With captions
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["mp4"],
            },
            {
                "subprop_type": "event",
                "subprop_key": "layout",
                "subprop_op": "is",
                "subprop_value": ["vertical"],
            },
        ],
    }

    vertical_with_captions = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update(
        {MetricsName.export_mp4_vertical_with_captions: vertical_with_captions}
    )

    # how many exports adobe premiere
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["premiere"],
            }
        ],
    }

    export_premiere = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_adobe: export_premiere})

    # how many exports davinci resolve
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["davinci"],
            }
        ],
    }

    export_davinci = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_resolve: export_davinci})

    # how many exports to fcp
    event_filters = {
        "event_type": AmplitudeEvents.user_click_export,
        "filters": [
            {
                "subprop_type": "event",
                "subprop_key": "exportType",
                "subprop_op": "is",
                "subprop_value": ["fcpxml"],
            }
        ],
    }
    export_fcp = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.export_fcp: export_fcp})

    # how many exports (total)
    event_filters = {"event_type": AmplitudeEvents.user_click_export}
    total_exports = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.totals,
        AMPLITUDE_AUTH,
    )
    exports_metrics.update({MetricsName.total_exports: total_exports})

    return ReportMetrics(
        data={
            MetricCategory.project_creation: MetricsData(
                metrics=project_creation_metrics
            ),
            MetricCategory.user_activity: MetricsData(metrics=user_activity_metrics),
            MetricCategory.share_activity: MetricsData(metrics=share_acitivity_metrics),
            MetricCategory.export: MetricsData(metrics=exports_metrics),
        }
    )


def parse_export_data(extracted_file_paths: List[str]) -> List[UserActivity]:
    # Summarize user activity
    user_store = UserActivityStore()

    for json_file in extracted_file_paths:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            for index, event in enumerate(data, start=1):
                if event.get("data_type") != "event":
                    continue

                user_id = (
                    event.get("user_id")
                    or f'Anonymous (Amplitude ID-{event.get("amplitude_id", index)})'
                )
                user_activity = user_store.get_activity(user_id)
                project = handle_export_event(event, user_activity)
                if project:
                    user_activity.add_project(project)
                    user_store.add_activity(user_activity)
    return user_store.get_all_activities()


def handle_export_event(event, user_activity: UserActivity):
    event_type = event.get("event_type")
    event_properties = event.get("event_properties")
    project_id = event_properties.get("projectId")
    project_name = event_properties.get("projectName")

    try:
        raw_files_info = event_properties.get("projectFileInfo", "[]")
        files_info = (
            json.loads(raw_files_info)
            if isinstance(raw_files_info, str)
            else raw_files_info
        )

        project_files_info = (
            ", ".join(
                [
                    f"{file.get('filename')} ({format_bytes(int(file.get('filesize', 0)))})"
                    for file in files_info
                ]
            )
            if files_info and isinstance(files_info, list)
            else None
        )
    except (json.JSONDecodeError, TypeError) as e:
        project_files_info = None
        print(f"Error parsing projectFileInfo: {e}")

    if event_type in {
        AmplitudeEvents.user_submit_prompt,
        AmplitudeEvents.user_click_prompt_suggest,
    }:
        prompt = (
            event_properties.get("prompt")
            if event_type == AmplitudeEvents.user_submit_prompt
            else event_properties.get("promptSuggest")
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_prompt(prompt)
        else:
            # Init new project with prompt
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[prompt],
                exports=[],
            )
        return project

    elif event_type == AmplitudeEvents.file_export_succeeded:
        layout = event_properties.get("layout")
        export_type = event_properties.get("exportType")
        ouput_duration_str = event_properties.get("outputDuration", None)
        readable_duration = (
            seconds_to_readable_time(float(ouput_duration_str))
            if ouput_duration_str
            else None
        )
        export_filename = event_properties.get("exportFileName")
        export_info = ExportInfo(
            export_layout=layout,
            export_type=export_type,
            export_file_name=export_filename,
            output_duration=readable_duration,
        )
        if user_activity.projects.get(project_id):
            # Update existing project
            project = user_activity.projects[project_id]
            project.file_info = project_files_info
            project.add_export(export_info)
        else:
            project = ProjectInfo(
                projetc_id=project_id,
                project_name=project_name,
                file_info=project_files_info,
                prompts=[],
                exports=[export_info],
            )
        return project
    else:
        return None


def handle_export_data_response(export_data) -> List[str]:
    os.makedirs(os.path.dirname(EXTRACTED_DATA_FILEPATH), exist_ok=True)
    # Write export data as zip
    with open(f"{EXPORT_FILEPATH}.zip", "wb") as f:
        f.write(export_data.content)
    # Extract data from zip archive
    with zipfile.ZipFile(f"{EXPORT_FILEPATH}.zip", "r") as zip_ref:
        zip_ref.extractall(f"/tmp/{EXPORT_FILENAME}")

    return os.listdir(EXTRACTED_DATA_FILEPATH)


def extract_export_data(files: List[str]) -> List[str]:
    ouput_file_paths = []

    # Extract export data to json
    for file in files:
        if file.endswith(".json.gz"):
            file_path = os.path.join(EXTRACTED_DATA_FILEPATH, file)

            extracted_data = []
            with gzip.open(file_path, "rt", encoding="utf-8") as gz_file:
                for line in gz_file:
                    json_obj = json.loads(line.strip())
                    extracted_data.append(json_obj)

            output_file = file.replace(".gz", "")
            output_path = os.path.join(EXTRACTED_DATA_FILEPATH, output_file)

            with open(output_path, "w", encoding="utf-8") as json_file:
                json.dump(extracted_data, json_file, indent=4)

            ouput_file_paths.append(EXTRACTED_DATA_FILEPATH + output_file)
            print(f"Extracted: {output_file}")
    return ouput_file_paths


async def handle_send_daily_users(
    date: str, user_store: List[UserActivity]
) -> Dict[str, str]:
    # Send report header
    header = f"🗓️ ({date}) Daily User Interactions\n"

    await send_slack_in_thread_message(
        {
            "blocks": [
                HeaderBlock(text=f"{header}").to_dict(),
                SectionBlock(text=" ").to_dict(),
            ]
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )

    users_thread = {}
    # Send user email message and init thread
    for idx, user_activity in enumerate(user_store, start=1):
        message = f"{idx}) 👤User: {user_activity.email}\n"
        # chat.postMessage has special rate limiting conditions (1msg/s)
        # https://api.slack.com/methods/chat.postMessage#rate_limiting
        await asyncio.sleep(1)
        res = await send_slack_in_thread_message(
            {"text": message}, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        users_thread[user_activity.email] = res.get("ts")

    # Send report footer
    await send_slack_in_thread_message(
        {
            "blocks": [
                SectionBlock(
                    text=f":checkered_flag: *End of Report:* {header}"
                ).to_dict(),
                SectionBlock(text=" ").to_dict(),
                DividerBlock(type="divider").to_dict(),
            ],
        },
        EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
    )
    return users_thread


async def handle_send_message_by_thread(
    users_thread: Dict[str, str], user_store: List[UserActivity]
):
    try:
        whitespace = "        "

        for idx, user_activity in enumerate(user_store, start=1):
            parsed_str = []

            for project_idx, project in enumerate(
                user_activity.projects.values(), start=1
            ):
                project_line = f":movie_camera: #{project_idx}. *Project name:* {project.project_name}\n"
                parsed_str.append(f"{project_line}")

                if project.file_info:
                    parsed_str.append(
                        f"{whitespace*2}- *File info:* {project.file_info}\n"
                    )

                if len(project.prompts) > 0:
                    parsed_str.append(f"{whitespace}:man-raising-hand: *Prompts:*\n")
                    for prompt_idx, prompt in enumerate(project.prompts, start=1):
                        parsed_str.append(f"{whitespace*2} - #{prompt_idx}: {prompt}\n")

                if len(project.exports) > 0:
                    parsed_str.append(f"{whitespace}:inbox_tray: *Exports:*\n")
                    for export_idx, export in enumerate(project.exports, start=1):
                        export_msg = (
                            f"{whitespace*2} - #{export_idx}:  Format: {export.export_layout}-{export.export_type}"
                            if export.export_layout
                            else f"{whitespace*2} - #{export_idx}:  Format: {export.export_type}"
                        )
                        if export.output_duration:
                            export_msg = (
                                f"{export_msg}, duration: {export.output_duration}"
                            )
                        parsed_str.append(f"{export_msg}\n")

            message_str = "".join(parsed_str)
            i = 0
            while i < len(message_str):
                end_index = min(i + SLACK_MESSAGE_LIMIT, len(message_str))
                if end_index < len(message_str) and "\n" in message_str[i:end_index]:
                    end_index = message_str.rfind("\n", i, end_index) + 1

                message = message_str[i:end_index]
                # chat.postMessage has special rate limiting conditions (1msg/s)
                # https://api.slack.com/methods/chat.postMessage#rate_limiting
                await asyncio.sleep(1)
                await send_slack_in_thread_message(
                    message={"text": message},
                    channel_id=EDDIE_ANALYTICS_DETAILS_CHANNEL_ID,
                    thread_ts=users_thread[user_activity.email],
                )
                i = end_index

    except Exception as e:
        print(f"Error handle_send_message_by_thread --errors: '{e}'")
        raise


async def handle_summarizes_user_interactions():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.strftime("%Y%m%d") + "T00"
    end_date = yesterday.strftime("%Y%m%d") + "T23"
    # format to readable date for send slack message
    formatted_date = yesterday.strftime("%Y-%m-%d")
    header = f"🗓️ ({formatted_date}) Daily User Interactions\n"

    export_data = export_amplitude_events(start_date, end_date)
    if not export_data:
        message = f"> - *Error when exporting events for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
        return

    try:
        ouptut_file_paths = handle_export_data_response(export_data)
        extracted_file_paths = extract_export_data(ouptut_file_paths)
        print(
            "Exported data, preparing data for daily summarizes user interactions analytics..."
        )
        data = parse_export_data(extracted_file_paths)
        print("Sending slack messsage...")
        if len(data) == 0:
            message = f"> - *Export data not found*"
            slack_template = build_slack_template(header, message)
            await send_slack_in_thread_message(
                slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
            )
        else:
            users_thread = await handle_send_daily_users(formatted_date, data)
            await handle_send_message_by_thread(users_thread, data)
    except Exception as e:
        print(
            "Unexpected error occur in daily summarizes user interactions event --errors: ",
            e,
        )
        message = f"> - *Error when preparing data for daily summarizes user interactions analytics*"
        slack_template = build_slack_template(header, message)
        await send_slack_in_thread_message(
            slack_template, EDDIE_ANALYTICS_DETAILS_CHANNEL_ID
        )
    finally:
        print("Sent slack message for daily summarizes user interactions analytics")
        free_up_disk_space(f"{EXPORT_FILEPATH}.zip")
        free_up_disk_space(EXTRACTED_DATA_FILEPATH)


def get_prompting_time_data(start_date, end_date) -> tuple:
    credentials = f"{AMPLITUDE_API_KEY}:{AMPLITUDE_SECRET_KEY}"
    AMPLITUDE_AUTH = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")

    # Average time taken for first eddie response for all users (Avarage the first time time taken when a new user prompts.)
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "filters": [
            {
                "subprop_type": "nth_time_hack",
                "subprop_key": "nth_time_performed",
                "subprop_op": "is",
                "subprop_value": ["1"],
            }
        ],
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_first_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )

    # Average time taken for all eddie responses
    event_filters = {
        "event_type": AmplitudeEvents.eddie_response,
        "group_by": [
            {
                "type": "event",
                "value": "timeTakenMs",
            }
        ],
    }
    avg_all_res_in_ms = sum_filtered_amplitude_events(
        event_filters,
        start_date,
        end_date,
        AmplitudeQueryMetrics.value_avg,
        AMPLITUDE_AUTH,
    )
    return avg_first_res_in_ms, avg_all_res_in_ms


def build_prompting_time_message(avg_first_res_in_ms, avg_all_res_in_ms):
    white_space = "            "
    message_str = "> 📝 *Average time taken for*:\n"
    message_str += f">{white_space}- *All eddie responses*: {seconds_to_readable_time(avg_all_res_in_ms/1000)}\n"
    message_str += f">{white_space}- *First eddie response for new users*: {seconds_to_readable_time(avg_first_res_in_ms/1000)}\n"
    return message_str


def handle_daily_prompting_time_report():
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # format to readable date for send slack message
    formated_date = yesterday.strftime("%Y-%m-%d")
    header = f"⏰ ({formated_date}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            yesterday.strftime("%Y%m%d"), yesterday.strftime("%Y%m%d")
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No daily metrics found for Date: {formated_date}*"
        else:
            print(
                f"Daily report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing daily prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing daily prompting time report*"
    print("Building slack message for daily prompting time report...")
    return build_slack_template(header, message)


def handle_weekly_prompting_time_report():
    monday_of_prev_week, sunday_of_prev_week = get_start_end_of_week_by_offset(
        week_offset=1
    )
    formatted_monday_of_prev_week = monday_of_prev_week.strftime("%Y-%m-%d")
    formatted_sunday_of_prev_week = sunday_of_prev_week.strftime("%Y-%m-%d")
    header = f"⏰ ({formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}) Eddie Prompting Time Analytics Report"

    try:
        # Query data for daily report
        avg_first_res_in_ms, avg_all_res_in_ms = get_prompting_time_data(
            monday_of_prev_week.strftime("%Y%m%d"),
            sunday_of_prev_week.strftime("%Y%m%d"),
        )
        if avg_first_res_in_ms is None and avg_all_res_in_ms is None:
            message = f"> - *No weekly metrics found for Date: {formatted_monday_of_prev_week} -> {formatted_sunday_of_prev_week}*"
        else:
            print(
                f"Weekly report prompting time.\n avg_first_res_in_ms: {avg_first_res_in_ms}, avg_all_res_in_ms: {avg_all_res_in_ms}"
            )
            message = build_prompting_time_message(
                avg_first_res_in_ms, avg_all_res_in_ms
            )
    except Exception as e:
        print(
            "Unexpected error occur when preparing weekly prompting time report --errors: ",
            e,
        )
        message = f"> - *Error when preparing weekly prompting time report*"
    print("Building slack message for weekly prompting time report...")
    return build_slack_template(header, message)
