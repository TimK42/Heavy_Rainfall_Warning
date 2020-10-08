import requests
import lineTool


# def lineNotifyMessage(token, msg):
#     headers = {
#         "Authorization": "Bearer " + token,
#         "Content-Type" : "application/x-www-form-urlencoded"
#     }

#     payload = {'message': msg}
#     r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)
#     return r.status_code


def _line_notify_pic(token, msg, pic_uri):
    url = "https://notify-api.line.me/api/notify"
    headers = {
        "Authorization": "Bearer " + token
    }
    payload = {'message': msg}
    files = {'imageFile': open(pic_uri, 'rb')}
    r = requests.post(url, headers=headers, params=payload, files=files, verify=False)
    return r.status_code


def _line_notify_sticker(token, msg, sticker_package_id, sticker_id):
    url = "https://notify-api.line.me/api/notify"
    headers = {
        "Authorization": "Bearer " + token
    }
    payload = {"message": msg, "sticker_package_id": sticker_package_id, 'sticker_id': sticker_id}
    r = requests.post(url, headers=headers, params=payload, verify=False)
    return r.status_code


def line_push_text(token, msg):
    lineTool.lineNotify(token, msg)
    return True


def line_push_pic(token, msg, pics):
    _line_notify_pic(token, msg, pics)
    return True


def line_push_sticker(token, msg, sticker_package_id, sticker_id):
    _line_notify_sticker(token, msg, sticker_package_id, sticker_id)
    return True