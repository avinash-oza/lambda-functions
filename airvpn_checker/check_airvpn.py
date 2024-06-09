import os

import requests


def get_airvpn_status(vpn_api_token, expected_session_count):
    url = "https://airvpn.org/api/"
    exit_code = 2
    message_text = "AirVPN\n"

    resp = requests.get(
        url, params={"service": "userinfo", "format": "json", "key": vpn_api_token}
    )
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        message_text += "Exception when getting status: {}".format(e)
        return message_text, exit_code

    data = resp.json()
    if "sessions" not in data:
        message_text += "Could not find any sessions"
    else:
        sessions = data["sessions"]
        session_count = len(sessions)
        message_text = f"Sessions currently connected: ({session_count}/{expected_session_count})\n"

        for one_session in sessions:
            message_text += f"{one_session['device_name']}({one_session['server_location']}, {one_session['server_country']})\n"

        if session_count == expected_session_count:
            exit_code = 0

    # at this point, we have a proper response
    message_text += "Days left: {},Expiry date: {}".format(
        data["user"]["expiration_days"], data["user"]["expiration_date"]
    )

    return message_text, exit_code


def lambda_handler(event, context):
    vpn_api_token = os.environ["AIRVPN_TOKEN"]
    expected_session_count = int(os.environ["EXPECTED_SESSION_COUNT"])
    status_text, exit_code = get_airvpn_status(vpn_api_token, expected_session_count)
    print(status_text, exit_code)


if __name__ == "__main__":
    lambda_handler(None, None)
