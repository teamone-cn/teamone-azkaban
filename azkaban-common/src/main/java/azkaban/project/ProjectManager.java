/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.project;

import static azkaban.Constants.EventReporterConstants.FLOW_NAME;
import static azkaban.Constants.EventReporterConstants.MODIFIED_BY;
import static azkaban.Constants.EventReporterConstants.PROJECT_NAME;
import static azkaban.Constants.FLOW_FILE_SUFFIX;
import static java.util.Objects.requireNonNull;

import azkaban.Constants;
import azkaban.executor.AlerterHolder;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionControllerUtils;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.flow.FlowResourceRecommendation;
import azkaban.project.ProjectLogEvent.EventType;
import azkaban.project.validator.ValidationReport;
import azkaban.scheduler.Schedule;
import azkaban.storage.ProjectStorageManager;
import azkaban.user.Permission;
import azkaban.user.Permission.Type;
import azkaban.user.User;
import azkaban.utils.Props;
import azkaban.utils.PropsUtils;
import azkaban.utils.SecurityTag;
import com.google.common.base.Joiner;
import com.google.common.io.Files;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class ProjectManager {

    private static final Logger logger = LoggerFactory.getLogger(ProjectManager.class);
    private final AzkabanProjectLoader azkabanProjectLoader;
    private final ProjectLoader projectLoader;
    private final Props props;
    private final boolean creatorDefaultPermissions;
    private final ProjectCache cache;
    private final AlerterHolder alerterHolder;

    @Inject
    public ProjectManager(final AzkabanProjectLoader azkabanProjectLoader,
                          final ProjectLoader loader,
                          final ProjectStorageManager projectStorageManager,
                          final Props props, final ProjectCache cache, final AlerterHolder alerterHolder
    ) {
        this.projectLoader = requireNonNull(loader);
        this.props = requireNonNull(props);
        this.azkabanProjectLoader = requireNonNull(azkabanProjectLoader);
        this.cache = requireNonNull(cache);
        this.creatorDefaultPermissions =
                props.getBoolean("creator.default.proxy", true);
        this.alerterHolder = alerterHolder;
        logger.info("Loading whitelisted projects.");
        loadProjectWhiteList();
        logger.info("ProjectManager instance created.");
    }

    public boolean hasFlowTrigger(final Project project, final Flow flow)
            throws IOException, ProjectManagerException {
        final String flowFileName = flow.getId() + ".flow";
        final int latestFlowVersion = this.projectLoader.getLatestFlowVersion(project.getId(), flow
                .getVersion(), flowFileName);
        if (latestFlowVersion > 0) {
            final File tempDir = com.google.common.io.Files.createTempDir();
            final File flowFile;
            try {
                flowFile = this.projectLoader
                        .getUploadedFlowFile(project.getId(), project.getVersion(),
                                flowFileName, latestFlowVersion, tempDir);

                final FlowTrigger flowTrigger = FlowLoaderUtils.getFlowTriggerFromYamlFile(flowFile);
                return flowTrigger != null;
            } catch (final Exception ex) {
                logger.error("error in getting flow file", ex);
                throw ex;
            } finally {
                FlowLoaderUtils.cleanUpDir(tempDir);
            }
        } else {
            return false;
        }
    }

    public Props getProps() {
        return this.props;
    }

    public List<Project> getUserProjects(final User user) {
        final ArrayList<Project> array = new ArrayList<>();
        for (final Project project : getProjects()) {
            final Permission perm = project.getUserPermission(user);

            if (perm != null
                    && (perm.isPermissionSet(Type.ADMIN) || perm
                    .isPermissionSet(Type.READ))) {
                array.add(project);
            }
        }
        return array;
    }

    public List<Project> getGroupProjects(final User user) {
        final List<Project> array = new ArrayList<>();
        for (final Project project : getProjects()) {
            if (project.hasGroupPermission(user, Type.READ)) {
                array.add(project);
            }
        }
        return array;
    }

    public List<Project> getUserProjectsByRegex(final User user, final String regexPattern) {
        final List<Project> array = new ArrayList<>();
        final List<Project> matches = getProjectsByRegex(regexPattern);
        for (final Project project : matches) {
            final Permission perm = project.getUserPermission(user);

            if (perm != null
                    && (perm.isPermissionSet(Type.ADMIN) || perm
                    .isPermissionSet(Type.READ))) {
                array.add(project);
            }
        }
        return array;
    }

    public List<Project> getProjects() {
        return new ArrayList<>(this.cache.getActiveProjects());
    }

    /**
     * This function matches the regex pattern with the names of all active projects, gets
     * corresponding ids and fetches the corresponding projects from the cache( cases : all projects
     * are present in cache / cache queries from DB and is updated).
     */
    public List<Project> getProjectsByRegex(final String regexPattern) {
        final Pattern pattern;
        try {
            pattern = Pattern.compile(regexPattern, Pattern.CASE_INSENSITIVE);
        } catch (final PatternSyntaxException e) {
            logger.error("Bad regex pattern {}", regexPattern);
            return Collections.emptyList();
        }
        return this.cache.getProjectsWithSimilarNames(pattern);
    }


    /**
     * Checks if a project is active using project_id. getProject(id) can also fetch he inactive
     * projects from DB. Thus we need to make sure project retrieved is present in the mapping which
     * consists of all the active projects. This map has key as project name in all the project cache
     * implementations.
     */
    public Boolean isActiveProject(final int id) {
        Project project = getProject(id);
        if (project == null) {
            return false;
        }
        project = getProject(project.getName());
        return project != null ? true : false;
    }

    /**
     * Fetch active project by project name. Queries the cache first then DB.
     */
    public Project getProject(final String name) {
        final Project fetchedProject = this.cache.getProjectByName(name).orElse(null);
        return fetchedProject;
    }

    /**
     * Fetch active/inactive project by project id. If active project not present in cache, fetches
     * from DB. Fetches inactive project from DB.
     */
    public Project getProject(final int id) {
        Project fetchedProject = null;
        try {
            fetchedProject = this.cache.getProjectById(id).orElse(null);
        } catch (final ProjectManagerException e) {
            logger.info("Could not load from store project with id:", id);
        }
        return fetchedProject;
    }

    public Project createProject(final String projectName, final String description,
                                 final User creator) throws ProjectManagerException {
        return createProject(projectName, description, creator, null);
    }

    public Project createProject(final String projectName, final String description,
                                 final User creator, final SecurityTag securityTag) throws ProjectManagerException {
        if (projectName == null || projectName.trim().isEmpty()) {
            throw new ProjectManagerException("Project name cannot be empty.");
        } else if (description == null || description.trim().isEmpty()) {
            throw new ProjectManagerException("Description cannot be empty.");
        } else if (creator == null) {
            throw new ProjectManagerException("Valid creator user must be set.");
        } else if (!projectName.matches("[a-zA-Z][a-zA-Z_0-9|-]*")) {
            throw new ProjectManagerException(
                    "Project names must start with a letter, followed by any number of letters, digits, '-' or '_'.");
        }

        final Project newProject;
        synchronized (this) {
            if (getProject(projectName) != null) {
                throw new ProjectManagerException("Project already exists.");
            }
            logger.info("Trying to create {} by user {}", projectName, creator.getUserId());
            newProject = this.projectLoader.createNewProject(projectName, description, creator, securityTag);
            this.cache.putProject(newProject);
        }

        if (this.creatorDefaultPermissions) {
            // Add permission to project
            this.projectLoader.updatePermission(newProject, creator.getUserId(),
                    new Permission(Permission.Type.ADMIN), false);

            // Add proxy user
            newProject.addProxyUser(creator.getUserId());
            try {
                updateProjectSetting(newProject);
            } catch (final ProjectManagerException e) {
                e.printStackTrace();
                throw e;
            }
        }

        this.projectLoader.postEvent(newProject, EventType.CREATED, creator.getUserId(),
                null);

        return newProject;
    }

    /**
     * Permanently delete all project files and properties data for all versions of a project and log
     * event in project_events table
     */
    public synchronized Project purgeProject(final Project project, final User deleter)
            throws ProjectManagerException {
        this.projectLoader.cleanOlderProjectVersion(project.getId(),
                project.getVersion() + 1, Collections.emptyList());
        this.projectLoader
                .postEvent(project, EventType.PURGE, deleter.getUserId(), String
                        .format("Purged versions before %d", project.getVersion() + 1));
        return project;
    }

    public synchronized Project removeProject(final Project project, final User deleter)
            throws ProjectManagerException {
        this.projectLoader.removeProject(project, deleter.getUserId());
        this.projectLoader.postEvent(project, EventType.DELETED, deleter.getUserId(),
                null);

        this.cache.removeProject(project);

        return project;
    }

    public void updateProjectDescription(final Project project, final String description,
                                         final User modifier) throws ProjectManagerException {
        this.projectLoader.updateDescription(project, description, modifier.getUserId());
        this.projectLoader.postEvent(project, EventType.DESCRIPTION,
                modifier.getUserId(), "Description changed to " + description);
    }

    public void updateProjectFeatureFlag(final Project project, final User modifier) throws ProjectManagerException {
        updateProjectSetting(project);
        this.projectLoader.postEvent(project, EventType.PROPERTY_OVERRIDE,
                modifier.getUserId(), "Update project feature flag to " + project.getFeatureFlags());
    }

    public List<ProjectLogEvent> getProjectEventLogs(final Project project,
                                                     final int results, final int skip) throws ProjectManagerException {
        return this.projectLoader.getProjectEvents(project, results, skip);
    }

    public Props getPropertiesFromFlowFile(final Flow flow, final String jobName, final String
            flowFileName, final int flowVersion, final Project project) throws ProjectManagerException {
        File tempDir = null;
        Props props = null;
        try {
            tempDir = Files.createTempDir();
            final File flowFile = this.projectLoader.getUploadedFlowFile(project.getId(), project
                    .getVersion(), flowFileName, flowVersion, tempDir);
            final String path =
                    jobName == null ? flow.getId() : flow.getId() + Constants.PATH_DELIMITER + jobName;
            props = FlowLoaderUtils.getPropsFromYamlFile(path, flowFile);
        } catch (final Exception e) {
            this.logger.error("Failed to get props from flow file. " + e);
        } finally {
            FlowLoaderUtils.cleanUpDir(tempDir);
        }
        return props;
    }

    public Props getProperties(final Project project, final Flow flow, final String jobName,
                               final String source) throws ProjectManagerException {
        if (FlowLoaderUtils.isAzkabanFlowVersion20(flow.getAzkabanFlowVersion())) {
            // Return the properties from the original uploaded flow file.
            return getPropertiesFromFlowFile(flow, jobName, source, 1, project);
        } else {
            return this.projectLoader.fetchProjectProperty(project, source);
        }
    }

    public Props getJobOverrideProperty(final Project project, final Flow flow, final String jobName,
                                        final String source) throws ProjectManagerException {
        if (FlowLoaderUtils.isAzkabanFlowVersion20(flow.getAzkabanFlowVersion())) {
            final int flowVersion = this.projectLoader
                    .getLatestFlowVersion(project.getId(), project.getVersion(), source);
            return getPropertiesFromFlowFile(flow, jobName, source, flowVersion, project);
        } else {
            return this.projectLoader
                    .fetchProjectProperty(project, jobName + Constants.JOB_OVERRIDE_SUFFIX);
        }
    }

    public void setJobOverrideProperty(final Project project, final Flow flow, final Props prop,
                                       final String jobName, final String source, final User modifier)  // todo: add IP?
            throws ProjectManagerException {
        File tempDir = null;
        Props oldProps = null;
        // For Azkaban event reporter
        String errorMessage = null;
        final Map<String, Object> eventData = new HashMap<>();
        addEventDataFromProject(project, eventData);

        if (FlowLoaderUtils.isAzkabanFlowVersion20(flow.getAzkabanFlowVersion())) {
            try {
                tempDir = Files.createTempDir();


                // 获取当前flow的版本
                final int flowVersion = this.projectLoader.getLatestFlowVersion(project.getId(), project
                        .getVersion(), source);

                // 获取当前flow的文件
                final File flowFile = this.projectLoader.getUploadedFlowFile(project.getId(), project
                        .getVersion(), source, flowVersion, tempDir);

                // 循环获取所有其他的flow的文件并会写入当前的 tempDir 下
                for (Flow singleFlow : project.getFlows()) {
                    System.out.println("写入了文件 -----" + singleFlow.getId());
                    if (!(singleFlow.getId() + FLOW_FILE_SUFFIX).equals(source)) {
                        this.projectLoader.getUploadedFlowFile(project.getId(), project
                                .getVersion(), singleFlow.getId() + FLOW_FILE_SUFFIX, flowVersion, tempDir);
                    }
                }

                final String path = flow.getId() + Constants.PATH_DELIMITER + jobName;
                oldProps = FlowLoaderUtils.getPropsFromYamlFile(path, flowFile);

                FlowLoaderUtils.setPropsInYamlFile(path, flowFile, prop);
                // 把新的 flow文件 上传到数据库保存
                this.projectLoader
                        .uploadFlowFile(project.getId(), project.getVersion(), flowFile, flowVersion + 1);

                // 还原其他flow的配置
                this.projectLoader
                        .uploadOtherFlowFile(project.getId(), project.getVersion(), source.split("\\.")[0],
                                flowVersion + 1);


            } catch (final Exception e) {
                this.logger.error("Failed to set job override property. " + e);
                errorMessage = e.toString();
            } finally {
                FlowLoaderUtils.cleanUpDir(tempDir);
            }
        } else {
            prop.setSource(jobName + Constants.JOB_OVERRIDE_SUFFIX);
            oldProps = this.projectLoader.fetchProjectProperty(project, prop.getSource());

            if (oldProps == null) {
                this.projectLoader.uploadProjectProperty(project, prop);
            } else {
                this.projectLoader.updateProjectProperty(project, prop);
            }
        }

        // Fill eventData with job property overridden event data
        eventData.put(MODIFIED_BY, modifier.getUserId());
        eventData.put(FLOW_NAME, flow.getId());
        eventData.put("jobOverridden", jobName);
        final String diffMessage = PropsUtils.getPropertyDiff(oldProps, prop);
        eventData.put("diffMessage", diffMessage);
        setProjectEventStatus(errorMessage, eventData);

        // Send job property overridden alert
        ExecutionControllerUtils.alertUserOnJobPropertyOverridden(project, flow, eventData, this.alerterHolder);
        // Fire project event listener
        project.fireEventListeners(ProjectEvent.create(project, azkaban.spi.EventType.JOB_PROPERTY_OVERRIDDEN, eventData));
        this.projectLoader.postEvent(project, EventType.PROPERTY_OVERRIDE,
                modifier.getUserId(), diffMessage);
        return;
    }

    /**
     * 在当前flow下增加job
     *
     * @param project
     * @param flow
     * @param prop
     * @param modifier
     * @throws ProjectManagerException
     */
    public void addJobInCurrentFlow(final Project project, final Flow flow, final Props prop,
                                    final String flowName, final User modifier)
            throws ProjectManagerException {
        File tempDir = null;
        Props oldProps = null;
        // For Azkaban event reporter
        String errorMessage = null;
        final Map<String, Object> eventData = new HashMap<>();
        addEventDataFromProject(project, eventData);

        if (FlowLoaderUtils.isAzkabanFlowVersion20(flow.getAzkabanFlowVersion())) {
            try {
                File newTempDir = Files.createTempDir();

                // 获取当前flow的版本
                final int flowVersion = this.projectLoader.getLatestFlowVersion(project.getId(), project
                        .getVersion(), flowName + FLOW_FILE_SUFFIX);

                // 获取当前flow的文件
                final File flowFile = this.projectLoader.getUploadedFlowFile(project.getId(), project
                        .getVersion(), flowName + FLOW_FILE_SUFFIX, flowVersion, newTempDir);


                // 循环获取所有其他的flow的文件并会写入当前的 tempDir 下
                for (Flow singleFlow : project.getFlows()) {
                    System.out.println("写入了文件 -----" + singleFlow.getId());
                    if (!singleFlow.getId().equals(flowName)) {
                        this.projectLoader.getUploadedFlowFile(project.getId(), project
                                .getVersion(), singleFlow.getId() + FLOW_FILE_SUFFIX, flowVersion, newTempDir);
                    }
                }

                final String path = flow.getId() + Constants.PATH_DELIMITER + flowName;

                // 创建新的临时文件

                String newPath = newTempDir.getPath() + "/" + flowFile.getName();
                File newDir = new File(newPath);

                // 添加新来的属性到临时文件中
                FlowLoaderUtils.addNewJobInYamlFile(path, flowFile, prop, newDir);

                final File newFlowFile = newDir;

                // 将 yaml 首行的内容删除掉
                tryDeleteFirstLineInFile(newFlowFile);

                // 获取到 flow加载器 并根据文件加载 flow
                FlowLoader flowLoader = new FlowLoaderFactory(new Props()).createFlowLoader(newTempDir);
                flowLoader.loadProjectFlow(project, newFlowFile.getParentFile());

                logger.info("获取到的新 error" + flowLoader.getErrors().toString());

                // 将获取到的 flowMap 设置到 project 中
                Map<String, Flow> flowMap = flowLoader.getFlowMap();
                project.setFlows(flowMap);

                // 把新的 flow文件 上传到数据库保存
                this.projectLoader
                        .uploadFlowFile(project.getId(), project.getVersion(), newFlowFile, flowVersion + 1);

                // 更新当前 flow 的配置
                this.projectLoader.updateFlow(project, project.getVersion(), flowMap.get(flowName));

                // 还原其他flow的配置
                this.projectLoader
                        .uploadOtherFlowFile(project.getId(), project.getVersion(), flowName, flowVersion + 1);


            } catch (final Exception e) {
                this.logger.error("Failed to add job. " + e);
                e.printStackTrace();
                errorMessage = e.toString();
            } finally {
                FlowLoaderUtils.cleanUpDir(tempDir);
            }
        } else {
            prop.setSource(flowName + Constants.JOB_OVERRIDE_SUFFIX);
            oldProps = this.projectLoader.fetchProjectProperty(project, prop.getSource());

            if (oldProps == null) {
                this.projectLoader.uploadProjectProperty(project, prop);
            } else {
                this.projectLoader.updateProjectProperty(project, prop);
            }
        }

        // Fill eventData with job property overridden event data
        eventData.put(MODIFIED_BY, modifier.getUserId());
        eventData.put(FLOW_NAME, flow.getId());
        eventData.put("jobAdded", flowName);

        setProjectEventStatus(errorMessage, eventData);

        // Send job property overridden alert
        ExecutionControllerUtils.alertUserOnJobPropertyOverridden(project, flow, eventData, this.alerterHolder);
        // Fire project event listener
        project.fireEventListeners(ProjectEvent.create(project, azkaban.spi.EventType.JOB_ADDED, eventData));
        this.projectLoader.postEvent(project, EventType.JOB_ADDED,
                modifier.getUserId(), flowName + " added " + prop.get("newJobName") + " success!");
        return;
    }


    /**
     * 在当前flow下删除传入的jobs
     *
     * @param project
     * @param flow
     * @param deleteJobNames
     * @param modifier
     * @throws ProjectManagerException
     */
    public void deleteJobsInCurrentFlow(final Project project, final Flow flow, final ArrayList<String> deleteJobNames,
                                        final String flowName, final User modifier)
            throws ProjectManagerException {
        File tempDir = null;
        // For Azkaban event reporter
        String errorMessage = null;
        final Map<String, Object> eventData = new HashMap<>();
        addEventDataFromProject(project, eventData);

        if (FlowLoaderUtils.isAzkabanFlowVersion20(flow.getAzkabanFlowVersion())) {
            try {
                tempDir = Files.createTempDir();

                // 获取当前flow的版本
                final int flowVersion = this.projectLoader.getLatestFlowVersion(project.getId(), project
                        .getVersion(), flowName + FLOW_FILE_SUFFIX);

                // 获取当前flow的文件
                final File flowFile = this.projectLoader.getUploadedFlowFile(project.getId(), project
                        .getVersion(), flowName + FLOW_FILE_SUFFIX, flowVersion, tempDir);


                File newTempDir = Files.createTempDir();

                // 循环获取所有其他的flow的文件并会写入当前的 tempDir 下
                for (Flow singleFlow : project.getFlows()) {
                    System.out.println("写入了文件 -----" + singleFlow.getId());
                    if (!singleFlow.getId().equals(flowName)) {
                        this.projectLoader.getUploadedFlowFile(project.getId(), project
                                .getVersion(), singleFlow.getId() + FLOW_FILE_SUFFIX, flowVersion, newTempDir);
                    }
                }

                final String path = flow.getId() + Constants.PATH_DELIMITER + flowName;

                // 创建新的临时文件
                String newPath = newTempDir.getPath() + "/" + flowFile.getName();
                File newDir = new File(newPath);

                // 删除对应的job并写入到yaml文件中
                FlowLoaderUtils.deleteJobsInYamlFile(path, flowFile, deleteJobNames, newDir);


                final File newFlowFile = newDir;

                // 将 yaml 首行的内容删除掉
                tryDeleteFirstLineInFile(newFlowFile);

                // 获取到 flow加载器 并根据文件加载 flow
                FlowLoader flowLoader = new FlowLoaderFactory(new Props()).createFlowLoader(newTempDir);
                flowLoader.loadProjectFlow(project, newFlowFile.getParentFile());

                logger.info("获取到的新 error" + flowLoader.getErrors().toString());

                // 将获取到的 flowMap 设置到 project 中
                Map<String, Flow> flowMap = flowLoader.getFlowMap();
                project.setFlows(flowMap);

                // 把新的 flow文件 上传到数据库保存
                this.projectLoader
                        .uploadFlowFile(project.getId(), project.getVersion(), newFlowFile, flowVersion + 1);

                // 更新当前 flow 的配置
                this.projectLoader.updateFlow(project, project.getVersion(), flowMap.get(flowName));

                // 还原其他flow的配置
                this.projectLoader
                        .uploadOtherFlowFile(project.getId(), project.getVersion(), flowName, flowVersion + 1);


            } catch (final Exception e) {
                this.logger.error("Failed to delete job in current flow. " + e);
                e.printStackTrace();
                errorMessage = e.toString();
            } finally {
                FlowLoaderUtils.cleanUpDir(tempDir);
            }
        }
        // Fill eventData with job property overridden event data
        eventData.put(MODIFIED_BY, modifier.getUserId());
        eventData.put(FLOW_NAME, flow.getId());
        eventData.put("jobDeleted", flowName);

        setProjectEventStatus(errorMessage, eventData);

        // Send job property overridden alert
        ExecutionControllerUtils.alertUserOnJobPropertyOverridden(project, flow, eventData, this.alerterHolder);
        // Fire project event listener
        project.fireEventListeners(ProjectEvent.create(project, azkaban.spi.EventType.JOB_DELETED, eventData));
        this.projectLoader.postEvent(project, EventType.JOB_DELETED,
                modifier.getUserId(), flowName + " deleted "
                        + String.join(",", deleteJobNames) + " success!");
        return;
    }


    /**
     * 在当前project下新增flow
     *
     * @param project
     * @param prop
     * @param modifier
     * @throws ProjectManagerException
     */
    public void addFlowInCurrentProject(final Project project, final Props prop, final User modifier)
            throws ProjectManagerException {
        File tempDir = null;
        // For Azkaban event reporter
        String errorMessage = null;
        final Map<String, Object> eventData = new HashMap<>();
        addEventDataFromProject(project, eventData);

        String newFlowName = null;
        Map<String, Flow> flowMap = null;
        try {

            // 获取到传入的新flow的名称
            newFlowName = prop.get("newFlowName");

            File newTempDir = Files.createTempDir();

            // 创建新的临时文件
            String newPath = newTempDir.getPath() + "/" + newFlowName + FLOW_FILE_SUFFIX;
            File newDirFlowFile = new File(newPath);

            // 添加新来的属性到临时文件中
            FlowLoaderUtils.addNewFlowInYamlFile(prop, newDirFlowFile);

            System.out.println("生成了flow");

            // 将 yaml 首行的内容删除掉
            tryDeleteFirstLineInFile(newDirFlowFile);


            // 获取当前flow的版本
            final int flowVersion = this.projectLoader.getOtherLatestFlowVersion(project.getId(), project
                    .getVersion(), newFlowName + FLOW_FILE_SUFFIX);

            // 循环获取所有其他的flow的文件并会写入当前的 tempDir 下
            for (Flow singleFlow : project.getFlows()) {
                System.out.println("写入了文件 -----" + singleFlow.getId());
                if (!singleFlow.getId().equals(newFlowName)) {
                    this.projectLoader.getUploadedFlowFile(project.getId(), project
                            .getVersion(), singleFlow.getId() + FLOW_FILE_SUFFIX, flowVersion, newTempDir);
                }
            }

            // 获取到 flow加载器 并根据文件加载 flow
            FlowLoader flowLoader = new FlowLoaderFactory(new Props()).createFlowLoader(newTempDir);
            flowLoader.loadProjectFlow(project, newDirFlowFile.getParentFile());

            logger.info("获取到的新 error" + flowLoader.getErrors().toString());

            // 将获取到的 flowMap 设置到 project 中
            flowMap = flowLoader.getFlowMap();
            project.setFlows(flowMap);

            logger.info("project.getFlows().size()----" + project.getFlows().size());

            // 上传当前的flow 到 project_flow表
            this.projectLoader.uploadFlow(project, project.getVersion(), flowMap.get(newFlowName));

            // 上传当前的flow的文件到 project_flow_files表
            this.projectLoader.uploadFlowFile(project.getId(), project.getVersion(), newDirFlowFile, flowVersion + 1);

            // 还原其他flow的配置
            this.projectLoader
                    .uploadOtherFlowFile(project.getId(), project.getVersion(), newFlowName, flowVersion + 1);

        } catch (final Exception e) {
            this.logger.error("Failed to add new flow. " + e);
            e.printStackTrace();
            errorMessage = e.toString();
        } finally {
            FlowLoaderUtils.cleanUpDir(tempDir);
        }


        // Fill eventData with job property overridden event data
        eventData.put(MODIFIED_BY, modifier.getUserId());
        eventData.put(FLOW_NAME, newFlowName);
        eventData.put("FlowAdded", newFlowName);

        setProjectEventStatus(errorMessage, eventData);

        // Send job property overridden alert
        ExecutionControllerUtils.alertUserOnJobPropertyOverridden(project, flowMap.get(newFlowName), eventData, this.alerterHolder);
        // Fire project event listener
        project.fireEventListeners(ProjectEvent.create(project, azkaban.spi.EventType.FLOW_ADDED, eventData));
        this.projectLoader.postEvent(project, EventType.FLOW_ADDED,
                modifier.getUserId(), newFlowName + " added success!");
        return;
    }


    /**
     * 在当前project下删除flow
     *
     * @param project
     * @param flowName
     * @param modifier
     * @throws ProjectManagerException
     */
    public void deleteFlowInCurrentProject(final Project project, final String flowName, final User modifier)
            throws ProjectManagerException {
        File tempDir = null;
        // For Azkaban event reporter
        String errorMessage = null;
        final Map<String, Object> eventData = new HashMap<>();
        addEventDataFromProject(project, eventData);

        Map<String, Flow> flowMap = null;
        try {
            File newTempDir = Files.createTempDir();

            // 获取当前flow的版本
            int flowVersion = this.projectLoader.getLatestFlowVersion(project.getId(), project.getVersion(),
                    flowName + FLOW_FILE_SUFFIX);

            logger.info("project.getFlows().size()----" + project.getFlows().size());


            File newProjectFile = new File(newTempDir + "/" + "flow20.project");
            BufferedWriter bufferedWriter = Files.newWriter(newProjectFile, Charset.defaultCharset());
            bufferedWriter.write("azkaban-flow-version: 2.0");
            bufferedWriter.close();

            // 循环获取 除开当前flowName的 所有flow的文件并会写入当前的 tempDir 下
            for (Flow singleFlow : project.getFlows()) {
                if (!singleFlow.getId().equals(flowName)) {
                    System.out.println("写入了文件 -----" + singleFlow.getId());
                    this.projectLoader.getUploadedFlowFile(project.getId(), project
                            .getVersion(), singleFlow.getId() + FLOW_FILE_SUFFIX, flowVersion, newTempDir);
                }
            }

            // 获取到 flow加载器 并根据文件加载 flow
            FlowLoader flowLoader = new FlowLoaderFactory(new Props()).createFlowLoader(newTempDir);
            flowLoader.loadProjectFlow(project, newTempDir);

            logger.info("获取到的新 error" + flowLoader.getErrors().toString());

            // 将获取到的 flowMap 设置到 project 中
            flowMap = flowLoader.getFlowMap();
            project.setFlows(flowMap);

            logger.info("project.getFlows().size()----" + project.getFlows().size());

            // 在表里删掉flow
            this.projectLoader.deleteFlowInProject(project.getId(), project.getVersion(), flowName);

            // 还原其他flow的配置
            this.projectLoader
                    .uploadOtherFlowFile(project.getId(), project.getVersion(), flowName, flowVersion + 1);

        } catch (final Exception e) {
            this.logger.error("Failed to add new flow. " + e);
            e.printStackTrace();
            errorMessage = e.toString();
        } finally {
            FlowLoaderUtils.cleanUpDir(tempDir);
        }


        // Fill eventData with job property overridden event data
        eventData.put(MODIFIED_BY, modifier.getUserId());
        eventData.put(FLOW_NAME, flowName);
        eventData.put("FlowDeleted", flowName);

        setProjectEventStatus(errorMessage, eventData);

        // Fire project event listener
        project.fireEventListeners(ProjectEvent.create(project, azkaban.spi.EventType.FLOW_DELETED, eventData));
        this.projectLoader.postEvent(project, EventType.FLOW_DELETED,
                modifier.getUserId(), flowName + " added success!");
        return;
    }


    public void updateProjectSetting(final Project project)
            throws ProjectManagerException {
        this.projectLoader.updateProjectSettings(project);
    }

    public void addProjectProxyUser(final Project project, final String proxyName,
                                    final User modifier) throws ProjectManagerException {
        logger.info("User {} adding proxy user {} to project {}", modifier.getUserId(), proxyName,
                project.getName());
        project.addProxyUser(proxyName);

        this.projectLoader.postEvent(project, EventType.PROXY_USER,
                modifier.getUserId(), "Proxy user " + proxyName + " is added to project.");
        updateProjectSetting(project);
    }

    public void removeProjectProxyUser(final Project project, final String proxyName,
                                       final User modifier) throws ProjectManagerException {
        logger.info("User {} removing proxy user {} from project {}", modifier.getUserId(),
                proxyName, project.getName());
        project.removeProxyUser(proxyName);

        this.projectLoader.postEvent(project, EventType.PROXY_USER,
                modifier.getUserId(), "Proxy user " + proxyName
                        + " has been removed form the project.");
        updateProjectSetting(project);
    }

    public void updateProjectPermission(final Project project, final String name,
                                        final Permission perm, final boolean group, final User modifier)  // todo: add IP?
            throws ProjectManagerException {
        logger.info("User {} updating permissions for project {} for {} {}", modifier.getUserId(),
                project.getName(), name, perm.toString());

        final Map<String, Object> eventData = new HashMap<>();
        addEventDataFromProject(project, eventData);
        eventData.put("modifiedBy", modifier.getUserId());
        eventData.put("permission", perm.toString());
        azkaban.spi.EventType eventType = azkaban.spi.EventType.USER_PERMISSION_CHANGED;
        if (group) {
            eventData.put("updatedUser", "null");
            eventData.put("updatedGroup", name);
            eventType = azkaban.spi.EventType.GROUP_PERMISSION_CHANGED;
        } else {
            eventData.put("updatedUser", name);
            eventData.put("updatedGroup", "null");
        }

        //Getting updated user and Group permissions as String
        final Map<String, String> updatedUserPermissionMap = new HashMap<>(project.getUserPermissions().size());
        final Map<String, String> updatedGroupPermissionMap = new HashMap<>(project.getGroupPermissions().size());

        project.getUserPermissions().forEach(el -> updatedUserPermissionMap.put(el.getFirst(), el.getSecond().toString()));
        project.getGroupPermissions().forEach(el -> updatedGroupPermissionMap.put(el.getFirst(), el.getSecond().toString()));

        String userPermissionMapAsString = Joiner.on(":").withKeyValueSeparator("=").join(updatedUserPermissionMap);
        String groupPermissionMapAsString = Joiner.on(":").withKeyValueSeparator("=").join(updatedGroupPermissionMap);

        eventData.put("updatedUserPermissions", userPermissionMapAsString);
        eventData.put("updatedGroupPermissions", groupPermissionMapAsString);

        String errorMessage = null;
        try {
            this.projectLoader.updatePermission(project, name, perm, group);
            if (group) {
                this.projectLoader.postEvent(project, EventType.GROUP_PERMISSION,
                        modifier.getUserId(), "Permission for group " + name + " set to "
                                + perm.toString());
            } else {
                this.projectLoader.postEvent(project, EventType.USER_PERMISSION,
                        modifier.getUserId(), "Permission for user " + name + " set to "
                                + perm.toString());
            }
        } catch (Exception e) {
            errorMessage = e.toString();
            throw e;
        } finally {
            setProjectEventStatus(errorMessage, eventData);
            // Fire Azkaban event listener
            project.fireEventListeners(ProjectEvent.create(project, eventType, eventData));
        }
    }

    public void removeProjectPermission(final Project project, final String name,
                                        final boolean group, final User modifier) throws ProjectManagerException {  // todo: Add IP?
        logger.info("User {} removing permissions for project {} for {}", modifier.getUserId(),
                project.getName(), name);

        final Map<String, Object> eventData = new HashMap<>();
        addEventDataFromProject(project, eventData);
        eventData.put("modifiedBy", modifier.getUserId());
        eventData.put("permission", "remove");
        azkaban.spi.EventType eventType = azkaban.spi.EventType.USER_PERMISSION_CHANGED;
        if (group) {
            eventData.put("updatedUser", "null");
            eventData.put("updatedGroup", name);
            eventType = azkaban.spi.EventType.GROUP_PERMISSION_CHANGED;
        } else {
            eventData.put("updatedUser", name);
            eventData.put("updatedGroup", "null");
        }

        String errorMessage = null;
        try {
            this.projectLoader.removePermission(project, name, group);
            if (group) {
                this.projectLoader.postEvent(project, EventType.GROUP_PERMISSION,
                        modifier.getUserId(), "Permission for group " + name + " removed.");
            } else {
                this.projectLoader.postEvent(project, EventType.USER_PERMISSION,
                        modifier.getUserId(), "Permission for user " + name + " removed.");
            }
        } catch (Exception e) {
            errorMessage = e.toString();
            throw e;
        } finally {
            setProjectEventStatus(errorMessage, eventData);
            // Fire Azkaban event listener
            project.fireEventListeners(ProjectEvent.create(project, eventType, eventData));
        }
    }

    /**
     * This method retrieves the uploaded project zip file from DB. A temporary file is created to
     * hold the content of the uploaded zip file. This temporary file is provided in the
     * ProjectFileHandler instance and the caller of this method should call method
     * {@ProjectFileHandler.deleteLocalFile} to delete the temporary file.
     *
     * @param version - latest version is used if value is -1
     * @return ProjectFileHandler - null if can't find project zip file based on project name and
     * version
     */
    public ProjectFileHandler getProjectFileHandler(final Project project, final int version)
            throws ProjectManagerException {
        return this.azkabanProjectLoader.getProjectFile(project, version);
    }

    public Map<String, ValidationReport> uploadProject(final Project project,
                                                       final File archive, final String fileType, final User uploader, final Props additionalProps,
                                                       final String uploaderIPAddr)
            throws ProjectManagerException, ExecutorManagerException {
        return this.azkabanProjectLoader
                .uploadProject(project, archive, fileType, uploader, additionalProps, uploaderIPAddr);
    }

    public void updateFlow(final Project project, final Flow flow)
            throws ProjectManagerException {
        this.projectLoader.updateFlow(project, flow.getVersion(), flow);
    }

    public FlowResourceRecommendation createFlowResourceRecommendation(final int projectId, final String flowId) throws ProjectManagerException {
        return this.projectLoader
                .createFlowResourceRecommendation(projectId, flowId);
    }

    public void updateFlowResourceRecommendation(final FlowResourceRecommendation flowResourceRecommendation)
            throws ProjectManagerException {
        this.projectLoader.updateFlowResourceRecommendation(flowResourceRecommendation);
    }

    public void postProjectEvent(final Project project, final EventType type, final String user,
                                 final String message) {
        this.projectLoader.postEvent(project, type, user, message);
    }

    public boolean loadProjectWhiteList() {
        if (this.props.containsKey(ProjectWhitelist.XML_FILE_PARAM)) {
            ProjectWhitelist.load(this.props);
            return true;
        }
        return false;
    }

    public void setProjectEventStatus(final String errorMessage, final Map<String, Object> eventData) {
        if (errorMessage == null) {
            eventData.put("projectEventStatus", "SUCCESS");
            eventData.put("errorMessage", "null");
        } else {
            eventData.put("projectEventStatus", "ERROR");
            eventData.put("errorMessage", errorMessage);
        }
    }

    public void postScheduleEvent(final Project project, final azkaban.spi.EventType type, final User user, final Schedule schedule, final String errorMessage) {
        final Map<String, Object> eventData = new HashMap<>();
        addEventDataFromProject(project, eventData);
        // Fill eventData with schedule event data
        eventData.put(MODIFIED_BY, user.getUserId());
        eventData.put(FLOW_NAME, schedule.getFlowName());
        eventData.put("firstScheduledExecutionTime", schedule.getFirstSchedTime());
        eventData.put("lastScheduledExecutionTime", schedule.getEndSchedTime());
        eventData.put("timezone", schedule.getTimezone().toString());
        eventData.put("cronExpression", schedule.getCronExpression());

        setProjectEventStatus(errorMessage, eventData);
        // Fire project schedule SLA event Listener
        project.fireEventListeners(ProjectEvent.create(project, type, eventData));  // todo: add IP?
    }

    public Props loadPropsForExecutableFlow(ExecutableFlow flow) throws ProjectManagerException {
        return FlowLoaderUtils.loadPropsForExecutableFlow(this.projectLoader, flow);
    }

    private void addEventDataFromProject(final Project project, final Map<String, Object> eventData) {
        eventData.put("projectId", project.getId());
        eventData.put(PROJECT_NAME, project.getName());
        eventData.put("projectVersion", project.getVersion());
    }

    /**
     * Try to delete first line in file
     *
     * @param inputFile file object
     */
    private static void tryDeleteFirstLineInFile(File inputFile) {
        try {

            File tempFile = new File(inputFile.getAbsolutePath() + "-bak");

            BufferedReader reader = Files.newReader(inputFile, Charset.defaultCharset());
            BufferedWriter writer = Files.newWriter(tempFile, Charset.defaultCharset());

            // 跳过第一行
            String currentLine;
            boolean firstLine = true;
            while ((currentLine = reader.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue;
                }
                writer.write(currentLine + System.getProperty("line.separator"));
            }

            reader.close();
            writer.close();

            // 删除原文件
            if (inputFile.delete()) {
                // 重命名临时文件为原文件名
                if (!tempFile.renameTo(inputFile)) {
                    logger.info("重命名失败");
                }
            } else {
                logger.info("删除原文件失败");
            }

            File newProjectFile = new File(inputFile.getParent() + "/" + "flow20.project");
            BufferedWriter bufferedWriter = Files.newWriter(newProjectFile, Charset.defaultCharset());
            bufferedWriter.write("azkaban-flow-version: 2.0");
            bufferedWriter.close();

        } catch (Exception e) {
            logger.warn("Failed to delete firstLine. file = " + inputFile.getAbsolutePath(), e);

        }
    }
}
