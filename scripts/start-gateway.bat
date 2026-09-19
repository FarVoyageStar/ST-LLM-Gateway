@echo off
rem Windows 便捷启动入口：切到项目根目录后以后台服务方式启动网关（等同于 npm start）。
rem Termux 环境不使用本脚本，启动由部署脚本 st_deploy.sh 负责。
cd /d "%~dp0.."
npm start
