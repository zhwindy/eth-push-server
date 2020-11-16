#!/usr/bin/env python

import smtplib

from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from core.globals import G_LOGGER


class Email:
    """邮件发送类"""
    def __init__(self, cfg):
        self.__mail_user = cfg.message.message_dict.get("mail_user")
        self.__mail_pass = cfg.message.message_dict.get("mail_pass")
        self.__mail_host = cfg.message.message_dict.get("mail_host")
        self.__mail_port = cfg.message.message_dict.get("mail_port")

    def send_mail(self, receiver, title, msg, mail_type="plain", file_paths=[], file_names=[], image_paths=None):
        """发送邮件"""
        assert receiver and isinstance(receiver, list)
        sender = self.__mail_user
        mail_type = mail_type.lower()
        if mail_type in ["plain", "html"]:
            message = MIMEText(msg, mail_type, "utf-8")
        elif mail_type in ["file", "image"]:
            message = MIMEMultipart()
        else:
            return False
        try:
            message["From"] = Header(self.__mail_user, "utf-8")
            message["To"] = Header(",".join(receiver), "utf-8")
            message["Subject"] = Header(title, "utf-8")

            if mail_type in ["file", "image"]:
                # 邮件正文内容
                if image_paths is not None:
                    message.attach(MIMEText(msg, "html", "utf-8"))
                    # 添加图片
                    if image_paths is not None:
                        for index, image_path in enumerate(image_paths, 1):
                            # 指定图片为当前目录
                            fp = open(image_path, "rb")
                            msg_image = MIMEImage(fp.read())
                            fp.close()
                            # 定义图片 ID，在 HTML 文本中引用
                            msg_image.add_header("Content-ID", "<image"+str(index)+">")
                            message.attach(msg_image)
                else:
                    message.attach(MIMEText(msg, "plain", "utf-8"))
                # 构造附件，传送filePath制定文件
                for filePath, fileName in zip(file_paths, file_names):
                    att = MIMEText(open(filePath, "rb").read(), "base64", "utf-8")
                    att["Content-Type"] = "application/octet-stream"
                    # 邮件中显示文件名
                    att["Content-Disposition"] = "attachment; filename="" + fileName + """
                    message.attach(att)
        except Exception as e:
            G_LOGGER.error(f"构造邮件发生错误,详情:{str(e)}")
            return False
        try:
            smtp = smtplib.SMTP_SSL(self.__mail_host, self.__mail_port)
            smtp.login(self.__mail_user, self.__mail_pass)
            smtp.sendmail(sender, receiver, message.as_string())
            smtp.quit()
        except Exception as e:
            G_LOGGER.error(f"发送邮件发生错误,详情:{str(e)}")
            return False
        return True
