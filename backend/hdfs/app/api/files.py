import io
import tempfile
import zipfile
from typing import Optional
import urllib.parse
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form, status, Response, Request
from fastapi.responses import StreamingResponse

from pathlib import Path

from app.core.config import cluster_registry
from app.core.security import get_current_user
from app.core.rate_limiter import get_client_ip
from app.core.audit import audit_log
from app.core.acl import can_access_cluster, is_cluster_read_only
from app.models.auth import UserInfo
from app.models.hdfs import DirectoryListingResponse, FilePreviewResponse, FileActionResponse, HdfsFileStatus
from app.services.hdfs_client import hdfs_service, WebHdfsException
from app.services.preview import preview_service

router = APIRouter(prefix="/api/v1/clusters/{cluster_id}/files", tags=["files"])


def sanitize_hdfs_path(path: str) -> str:
    """
    Нормализует и валидирует путь HDFS, предотвращая Path Traversal (CWE-22) и инъекции.
    """
    if not path:
        return "/"
    if "\0" in path:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Недопустимый символ в пути (null byte)")
    clean = "/" + path.strip("/")
    parts = [p for p in clean.split("/") if p]
    if any(p in (".", "..") for p in parts):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Относительные переходы (..) в путях запрещены"
        )
    return "/" + "/".join(parts)


def _get_cluster_and_validate(cluster_id: str, current_user: UserInfo):
    cluster = cluster_registry.get(cluster_id)
    if not cluster:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Кластер с id '{cluster_id}' не найден")

    if not can_access_cluster(cluster, current_user.username, current_user.groups):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail=f"У вас нет прав доступа к кластеру '{cluster.name}'"
        )

    return cluster


@router.get("", response_model=DirectoryListingResponse)
async def list_files(
    cluster_id: str, path: str = Query(default="/"), current_user: UserInfo = Depends(get_current_user)
):
    cluster = _get_cluster_and_validate(cluster_id, current_user)
    client = hdfs_service.get_client(cluster)

    # Нормализация пути
    clean_path = sanitize_hdfs_path(path)
    parent_path = None
    if clean_path != "/":
        parts = clean_path.rstrip("/").rsplit("/", 1)
        parent_path = parts[0] if parts[0] else "/"

    try:
        statuses = await client.list_status(clean_path, do_as_user=current_user.username)
    except WebHdfsException as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)

    total_files = sum(1 for s in statuses if s.type == "FILE")
    total_dirs = sum(1 for s in statuses if s.type == "DIRECTORY")
    total_size = sum(s.length for s in statuses if s.type == "FILE")

    is_ro = is_cluster_read_only(cluster, current_user.username, current_user.groups)

    return DirectoryListingResponse(
        cluster_id=cluster_id,
        path=clean_path,
        parent_path=parent_path,
        files=statuses,
        total_files=total_files,
        total_directories=total_dirs,
        total_size=total_size,
        can_write=not is_ro,
        can_read=True,
    )


@router.get("/preview", response_model=FilePreviewResponse)
async def preview_file(cluster_id: str, path: str = Query(...), current_user: UserInfo = Depends(get_current_user)):
    cluster = _get_cluster_and_validate(cluster_id, current_user)
    client = hdfs_service.get_client(cluster)
    clean_path = sanitize_hdfs_path(path)

    max_bytes = cluster.preview_max_bytes
    try:
        file_status = await client.get_file_status(clean_path, do_as_user=current_user.username)
        total_size = file_status.length
        content = await client.get_file_content(
            clean_path, do_as_user=current_user.username, offset=0, length=max_bytes + 1
        )
    except WebHdfsException as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)

    preview_bytes = content[:max_bytes]
    return preview_service.generate_preview(clean_path, cluster_id, preview_bytes, total_size, max_bytes)


# Лимиты безопасности для предотвращения DoS / OOM (CWE-400)
MAX_ZIP_FILES = 1000
MAX_ZIP_TOTAL_BYTES = 500 * 1024 * 1024  # 500 МБ
MAX_UPLOAD_ARCHIVE_BYTES = 500 * 1024 * 1024  # 500 МБ


async def create_directory_zip(client, base_path: str, username: str) -> tempfile.SpooledTemporaryFile:
    """
    Рекурсивно собирает содержимое каталога HDFS в ZIP-архив с потоковой записью чанками.
    Имеет встроенную защиту от DoS/OOM по количеству файлов и размеру.
    """
    spooled_file = tempfile.SpooledTemporaryFile(max_size=20 * 1024 * 1024, mode="w+b")
    dir_name = base_path.split("/")[-1] if base_path != "/" else "root"
    total_files = 0
    total_bytes = 0

    try:
        with zipfile.ZipFile(spooled_file, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"{dir_name}/", b"")

            async def _walk(curr_path: str, rel_prefix: str):
                nonlocal total_files, total_bytes
                items = await client.list_status(curr_path, username)
                for item in items:
                    sub_path = f"{curr_path.rstrip('/')}/{item.pathSuffix}"
                    sub_rel = f"{rel_prefix}/{item.pathSuffix}"

                    if item.type == "DIRECTORY":
                        zf.writestr(f"{sub_rel}/", b"")
                        await _walk(sub_path, sub_rel)
                    else:
                        total_files += 1
                        total_bytes += item.length
                        if total_files > MAX_ZIP_FILES:
                            raise HTTPException(
                                status_code=status.HTTP_400_BAD_REQUEST,
                                detail=f"Каталог содержит более {MAX_ZIP_FILES} файлов. Архивация заблокирована для защиты от перегрузки.",
                            )
                        if total_bytes > MAX_ZIP_TOTAL_BYTES:
                            raise HTTPException(
                                status_code=status.HTTP_400_BAD_REQUEST,
                                detail="Суммарный объем файлов каталога превышает лимит 500 МБ. Скачивайте файлы напрямую.",
                            )

                        # Потоковая запись файла в zip-архив чанками без загрузки в память
                        with zf.open(sub_rel, mode="w", force_zip64=True) as dest_f:
                            async for chunk in client.open_stream(sub_path, username):
                                dest_f.write(chunk)

            await _walk(base_path, dir_name)

        spooled_file.seek(0)
        return spooled_file
    except Exception:
        spooled_file.close()
        raise


@router.get("/download")
async def download_file(
    cluster_id: str, request: Request, path: str = Query(...), current_user: UserInfo = Depends(get_current_user)
):
    cluster = _get_cluster_and_validate(cluster_id, current_user)
    client = hdfs_service.get_client(cluster)
    clean_path = sanitize_hdfs_path(path)

    try:
        file_status = await client.get_file_status(clean_path, do_as_user=current_user.username)
    except WebHdfsException as e:
        audit_log(
            action="FILE_DOWNLOAD",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "path": clean_path, "error": e.message},
            status="FAILED",
        )
        raise HTTPException(status_code=e.status_code, detail=e.message)

    # Если скачивается каталог, собираем и отдаем ZIP-архив
    if file_status.type == "DIRECTORY":
        if clean_path == "/":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Скачивание корневого каталога (/) целиком в виде ZIP-архива запрещено",
            )

        folder_name = clean_path.split("/")[-1]
        zip_filename = f"{folder_name}.zip"
        quoted_filename = urllib.parse.quote(zip_filename)
        headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{quoted_filename}",
            "Content-Type": "application/zip",
        }
        try:
            zip_file = await create_directory_zip(client, clean_path, current_user.username)
            audit_log(
                action="DIRECTORY_DOWNLOAD_ZIP",
                username=current_user.username,
                client_ip=get_client_ip(request),
                details={"cluster_id": cluster_id, "path": clean_path, "filename": zip_filename},
                status="SUCCESS",
            )

            async def iter_zip():
                try:
                    while True:
                        chunk = zip_file.read(65536)
                        if not chunk:
                            break
                        yield chunk
                finally:
                    zip_file.close()

            return StreamingResponse(iter_zip(), headers=headers, media_type="application/zip")
        except WebHdfsException as e:
            audit_log(
                action="DIRECTORY_DOWNLOAD_ZIP",
                username=current_user.username,
                client_ip=get_client_ip(request),
                details={"cluster_id": cluster_id, "path": clean_path, "error": e.message},
                status="FAILED",
            )
            raise HTTPException(status_code=e.status_code, detail=e.message)

    # Стриминг отдельного файла
    filename = clean_path.split("/")[-1]
    quoted_filename = urllib.parse.quote(filename)
    headers = {
        "Content-Disposition": f"attachment; filename*=UTF-8''{quoted_filename}",
        "Content-Type": "application/octet-stream",
    }

    try:
        stream = client.open_stream(clean_path, do_as_user=current_user.username)
        audit_log(
            action="FILE_DOWNLOAD",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "path": clean_path, "size": file_status.length},
            status="SUCCESS",
        )
        return StreamingResponse(stream, headers=headers)
    except WebHdfsException as e:
        audit_log(
            action="FILE_DOWNLOAD",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "path": clean_path, "error": e.message},
            status="FAILED",
        )
        raise HTTPException(status_code=e.status_code, detail=e.message)


@router.post("/upload", response_model=FileActionResponse)
async def upload_file(
    cluster_id: str,
    request: Request,
    path: str = Form(...),
    file: UploadFile = File(...),
    relative_path: Optional[str] = Form(None),
    overwrite: bool = Form(default=True),
    current_user: UserInfo = Depends(get_current_user),
):
    cluster = _get_cluster_and_validate(cluster_id, current_user)
    if is_cluster_read_only(cluster, current_user.username, current_user.groups):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Кластер доступен только для чтения")

    client = hdfs_service.get_client(cluster)
    target_dir = sanitize_hdfs_path(path)

    # Если передан relative_path, сохраняем вложенную структуру каталогов с защитой от traversal
    rel = relative_path.strip("/") if relative_path else None
    if rel:
        safe_parts = [p for p in rel.replace("\\", "/").split("/") if p and p not in (".", "..")]
        safe_rel = "/".join(safe_parts)
        full_path = f"{target_dir}/{safe_rel}" if target_dir != "/" else f"/{safe_rel}"
    else:
        # Защита от Path Traversal (CWE-22): извлекаем только базовое имя файла
        raw_name = file.filename or "uploaded_file"
        safe_filename = Path(raw_name).name
        if not safe_filename or safe_filename in (".", ".."):
            safe_filename = "uploaded_file"
        full_path = f"{target_dir}/{safe_filename}" if target_dir != "/" else f"/{safe_filename}"

    # Создаем промежуточные папки, если необходимо
    parent_dir = full_path.rsplit("/", 1)[0]
    if parent_dir and parent_dir != target_dir and parent_dir != "/":
        try:
            await client.mkdirs(parent_dir, do_as_user=current_user.username)
        except Exception:
            pass

    # Потоковая передача содержимого чанками по 64 КБ без буферизации файла целиком в RAM
    async def file_streamer():
        while chunk := await file.read(65536):
            yield chunk

    try:
        await client.create_file(full_path, file_streamer(), do_as_user=current_user.username, overwrite=overwrite)
        audit_log(
            action="FILE_UPLOAD",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "path": full_path, "overwrite": overwrite},
            status="SUCCESS",
        )
        return FileActionResponse(
            success=True, message=f"Файл '{full_path.split('/')[-1]}' успешно загружен", path=full_path
        )
    except WebHdfsException as e:
        audit_log(
            action="FILE_UPLOAD",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "path": full_path, "error": e.message},
            status="FAILED",
        )
        raise HTTPException(status_code=e.status_code, detail=e.message)


# Лимиты распаковки архивов (защита от Zip Bomb / исчерпания ресурсов)
MAX_EXTRACT_FILES = 2000
MAX_EXTRACT_TOTAL_BYTES = 1024 * 1024 * 1024  # 1 ГБ


@router.post("/upload-archive", response_model=FileActionResponse)
async def upload_archive(
    cluster_id: str,
    request: Request,
    path: str = Form(...),
    file: UploadFile = File(...),
    overwrite: bool = Form(default=True),
    current_user: UserInfo = Depends(get_current_user),
):
    """
    Загрузка и автоматическая распаковка ZIP-архива в целевую директорию HDFS с сохранением структуры.
    Включает потоковое чтение, контроль максимального размера архива и распаковку чанками (защита от DoS/OOM/Zip Bomb).
    """
    cluster = _get_cluster_and_validate(cluster_id, current_user)
    if is_cluster_read_only(cluster, current_user.username, current_user.groups):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Кластер доступен только для чтения")

    client = hdfs_service.get_client(cluster)
    target_dir = sanitize_hdfs_path(path)

    temp_archive = tempfile.SpooledTemporaryFile(max_size=10 * 1024 * 1024, mode="w+b")
    try:
        uploaded_bytes = 0
        while True:
            chunk = await file.read(65536)
            if not chunk:
                break
            uploaded_bytes += len(chunk)
            if uploaded_bytes > MAX_UPLOAD_ARCHIVE_BYTES:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Размер загруженного архива превышает лимит {MAX_UPLOAD_ARCHIVE_BYTES // (1024 * 1024)} МБ",
                )
            temp_archive.write(chunk)

        temp_archive.seek(0)

        if not zipfile.is_zipfile(temp_archive):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Загруженный файл не является корректным ZIP-архивом"
            )

        temp_archive.seek(0)
        files_created = 0
        dirs_created = set()

        with zipfile.ZipFile(temp_archive, mode="r") as zf:
            infolist = zf.infolist()
            if len(infolist) > MAX_EXTRACT_FILES:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Архив содержит слишком много файлов (максимум {MAX_EXTRACT_FILES})",
                )

            total_uncompressed = sum(info.file_size for info in infolist)
            if total_uncompressed > MAX_EXTRACT_TOTAL_BYTES:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Распакованный размер архива превышает лимит безопасности 1 ГБ",
                )

            for info in infolist:
                safe_filename = info.filename.replace("\\", "/").strip("/")
                parts = [p for p in safe_filename.split("/") if p and p not in (".", "..")]
                if not parts:
                    continue
                clean_rel = "/".join(parts)
                dst_item_path = f"{target_dir}/{clean_rel}" if target_dir != "/" else f"/{clean_rel}"

                if info.is_dir():
                    if dst_item_path not in dirs_created:
                        await client.mkdirs(dst_item_path, do_as_user=current_user.username)
                        dirs_created.add(dst_item_path)
                else:
                    parent_d = dst_item_path.rsplit("/", 1)[0]
                    if parent_d and parent_d not in dirs_created and parent_d != target_dir and parent_d != "/":
                        await client.mkdirs(parent_d, do_as_user=current_user.username)
                        dirs_created.add(parent_d)

                    # Асинхронный генератор потока для передачи файла чанками без загрузки в RAM
                    def make_file_stream(zf_inst, member_info):
                        async def _stream():
                            with zf_inst.open(member_info, mode="r") as src_f:
                                while True:
                                    f_chunk = src_f.read(65536)
                                    if not f_chunk:
                                        break
                                    yield f_chunk

                        return _stream()

                    await client.create_file(
                        dst_item_path, make_file_stream(zf, info), do_as_user=current_user.username, overwrite=overwrite
                    )
                    files_created += 1

        audit_log(
            action="ARCHIVE_UPLOAD_EXTRACT",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "path": target_dir, "files_created": files_created},
            status="SUCCESS",
        )
        return FileActionResponse(
            success=True,
            message=f"Архив успешно распакован: создано {files_created} файлов в '{target_dir}'",
            path=target_dir,
        )
    except WebHdfsException as e:
        audit_log(
            action="ARCHIVE_UPLOAD_EXTRACT",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "path": target_dir, "error": e.message},
            status="FAILED",
        )
        raise HTTPException(status_code=e.status_code, detail=e.message)
    finally:
        temp_archive.close()


@router.post("/mkdir", response_model=FileActionResponse)
async def make_directory(
    cluster_id: str, request: Request, path: str = Query(...), current_user: UserInfo = Depends(get_current_user)
):
    cluster = _get_cluster_and_validate(cluster_id, current_user)
    if is_cluster_read_only(cluster, current_user.username, current_user.groups):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Кластер доступен только для чтения")

    client = hdfs_service.get_client(cluster)
    clean_path = sanitize_hdfs_path(path)
    if clean_path == "/":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Нельзя создать корневой каталог")

    try:
        await client.mkdirs(clean_path, do_as_user=current_user.username)
        audit_log(
            action="DIRECTORY_CREATE",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "path": clean_path},
            status="SUCCESS",
        )
        return FileActionResponse(success=True, message=f"Директория '{clean_path}' успешно создана", path=clean_path)
    except WebHdfsException as e:
        audit_log(
            action="DIRECTORY_CREATE",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "path": clean_path, "error": e.message},
            status="FAILED",
        )
        raise HTTPException(status_code=e.status_code, detail=e.message)


@router.post("/rename", response_model=FileActionResponse)
async def rename_path(
    cluster_id: str,
    request: Request,
    src: str = Query(...),
    dst: str = Query(...),
    current_user: UserInfo = Depends(get_current_user),
):
    cluster = _get_cluster_and_validate(cluster_id, current_user)
    if is_cluster_read_only(cluster, current_user.username, current_user.groups):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Кластер доступен только для чтения")

    clean_src = sanitize_hdfs_path(src)
    clean_dst = sanitize_hdfs_path(dst)
    if clean_src == "/" or clean_dst == "/":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Нельзя переименовать корневой каталог")

    client = hdfs_service.get_client(cluster)
    try:
        await client.rename(clean_src, clean_dst, do_as_user=current_user.username)
        audit_log(
            action="PATH_RENAME",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "src": clean_src, "dst": clean_dst},
            status="SUCCESS",
        )
        return FileActionResponse(success=True, message="Успешно переименовано", path=clean_dst)
    except WebHdfsException as e:
        audit_log(
            action="PATH_RENAME",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "src": clean_src, "dst": clean_dst, "error": e.message},
            status="FAILED",
        )
        raise HTTPException(status_code=e.status_code, detail=e.message)


@router.delete("/delete", response_model=FileActionResponse)
async def delete_path(
    cluster_id: str,
    request: Request,
    path: str = Query(...),
    recursive: bool = Query(default=False),
    current_user: UserInfo = Depends(get_current_user),
):
    cluster = _get_cluster_and_validate(cluster_id, current_user)
    if is_cluster_read_only(cluster, current_user.username, current_user.groups):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Кластер доступен только для чтения")

    client = hdfs_service.get_client(cluster)
    clean_path = sanitize_hdfs_path(path)
    if clean_path == "/":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Удаление корневой директории (/) запрещено"
        )

    try:
        await client.delete(clean_path, do_as_user=current_user.username, recursive=recursive)
        audit_log(
            action="PATH_DELETE",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "path": clean_path, "recursive": recursive},
            status="SUCCESS",
        )
        return FileActionResponse(success=True, message=f"'{clean_path}' успешно удален", path=clean_path)
    except WebHdfsException as e:
        audit_log(
            action="PATH_DELETE",
            username=current_user.username,
            client_ip=get_client_ip(request),
            details={"cluster_id": cluster_id, "path": clean_path, "recursive": recursive, "error": e.message},
            status="FAILED",
        )
        raise HTTPException(status_code=e.status_code, detail=e.message)
