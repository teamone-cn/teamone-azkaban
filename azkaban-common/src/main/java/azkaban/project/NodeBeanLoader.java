/*
 * Copyright 2017 LinkedIn Corp.
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
 *
 */

package azkaban.project;

import static com.google.common.base.Preconditions.checkArgument;

import azkaban.Constants;
import azkaban.Constants.FlowTriggerProps;
import azkaban.utils.Props;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import java.io.File;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.quartz.CronExpression;
import org.yaml.snakeyaml.Yaml;

/**
 * Loads NodeBean from YAML files.
 */
public class NodeBeanLoader {

    public NodeBean load(final File flowFile) throws Exception {
        checkArgument(flowFile != null && flowFile.exists());
        checkArgument(flowFile.getName().endsWith(Constants.FLOW_FILE_SUFFIX));

        final NodeBean nodeBean = new Yaml().loadAs(new FileInputStream(flowFile), NodeBean.class);
        if (nodeBean == null) {
            throw new ProjectManagerException(
                    "Failed to load flow file " + flowFile.getName() + ". Node bean is null .");
        }

        for (NodeBean jobNode : nodeBean.getNodes()) {
            System.out.println(jobNode.toString());
        }


        nodeBean.setName(getFlowName(flowFile));
        nodeBean.setType(Constants.FLOW_NODE_TYPE);
        return nodeBean;
    }

    /**
     * 加载 flow file 并添加一个新的job
     *
     * @param flowFile
     * @return
     * @throws Exception
     */
    public NodeBean loadAndAdd(final File flowFile, Props prop) throws Exception {
        checkArgument(flowFile != null && flowFile.exists());
        checkArgument(flowFile.getName().endsWith(Constants.FLOW_FILE_SUFFIX));

        NodeBean nodeBean = new Yaml().loadAs(new FileInputStream(flowFile), NodeBean.class);
        if (nodeBean == null) {
            throw new ProjectManagerException(
                    "Failed to load flow file " + flowFile.getName() + ". Node bean is null .");
        }
        // 新建一个job的节点
        NodeBean jobNode = new NodeBean();

        // job类型的属性的设置
        HashMap<String, String> objectObjectHashMap = new HashMap<>();
        objectObjectHashMap.put(prop.get("type"), prop.get(prop.get("type")));

        // job依赖的设置
        ArrayList<String> dependOnList = new ArrayList<>();
        if (StringUtils.isNotEmpty(prop.get("dependOn"))) {
            for (String depend : prop.get("dependOn").split(",")) {
                dependOnList.add(depend);
            }
            jobNode.setDependsOn(dependOnList);
        }

        jobNode.setName(prop.get("newJobName"));
        jobNode.setType(prop.get("type"));
        jobNode.setConfig(objectObjectHashMap);

        nodeBean.getNodes().add(jobNode);

        nodeBean.setName(getFlowName(flowFile));
        nodeBean.setType(Constants.FLOW_NODE_TYPE);

        return nodeBean;
    }

    /**
     * 生成一个Flow File
     *
     * @return
     * @throws Exception
     */
    public NodeBean loadAndGenerate(Props prop)  {

        NodeBean nodeBean =  new NodeBean();

        ArrayList<NodeBean> jobNodes = new ArrayList<>();
        NodeBean jobNode = new NodeBean();
        jobNode.setType("noop");
        jobNode.setName("init");

        jobNodes.add(jobNode);

        nodeBean.setNodes(jobNodes);
        nodeBean.setName(prop.get("newFlowName"));
        nodeBean.setType(Constants.FLOW_NODE_TYPE);

        return nodeBean;
    }



    /**
     * 加载 flow file 并删除 jobs，并处理各种依赖的关系
     *
     * @param flowFile
     * @return
     * @throws Exception
     */
    public NodeBean loadAndDelete(final File flowFile, ArrayList<String> deleteJobNames) throws Exception {
        checkArgument(flowFile != null && flowFile.exists());
        checkArgument(flowFile.getName().endsWith(Constants.FLOW_FILE_SUFFIX));

        NodeBean nodeBean = new Yaml().loadAs(new FileInputStream(flowFile), NodeBean.class);
        if (nodeBean == null) {
            throw new ProjectManagerException(
                    "Failed to load flow file " + flowFile.getName() + ". Node bean is null .");
        }

        // 获取到目前已经存在的所有的 nodeBean
        List<NodeBean> currentNodeBeans = nodeBean.getNodes();

        // 要删除的节点列表
        List<NodeBean> needDeleteNodeBeans = new ArrayList<>();

        // 要删除删除节点的依赖的节点列表
        List<NodeBean> needDeleteDependBeans = new ArrayList<>();

        // 在验证请求参数的时候已经验证过名称的有效性，这里不对jobName再进行验证
        // 根据需要删除的job的名称遍历出节点
        for (String deleteJobName : deleteJobNames) {
            System.out.println("要删除的job的名称是" + deleteJobName);

            for (NodeBean bean : currentNodeBeans) {
                System.out.println("bean.getName()----" + bean.getName());

                // 先找到要删除的节点并放到 要删除的节点列表 中
                if (bean.getName().equals(deleteJobName)) {
                    needDeleteNodeBeans.add(bean);
                }

                // 再找到要删除 删除节点的依赖的节点 加入到节点列表中
                if (ObjectUtils.isNotEmpty(bean.getDependsOn()) &&
                        bean.getDependsOn().size() > 0 &&
                        bean.getDependsOn().contains(deleteJobName)) {
                    needDeleteDependBeans.add(bean);
                }
            }
        }

        // 根据需要删除的节点列表获取这些节点dependOn的前置节点，并构建 HashMap
        Map<String, List<String>> preNodeBeans = new HashMap<>();

        for (NodeBean delete : needDeleteNodeBeans) {
            String deleteName = delete.getName();

            System.out.println("删除的名称为" + deleteName);

            preNodeBeans.put(deleteName, delete.getDependsOn());
        }

        // 在需要删除的后继节点中 删除掉删掉的节点名称，并添加当前删掉的节点的名称的前置节点
        for (NodeBean delete : needDeleteNodeBeans) {
            for (NodeBean deleteDepend : needDeleteDependBeans) {

                List<String> newDependsOn = new ArrayList<>();
                for (String depend : deleteDepend.getDependsOn()) {
                    if (depend.equals(delete.getName())) {
                        System.out.println("depend为：" + depend);
                        System.out.println("preNodeBeans为：" + preNodeBeans);
                        List<String> preDependNodeBeans = preNodeBeans.get(delete.getName());
                        if (ObjectUtils.isNotEmpty(preDependNodeBeans) && preDependNodeBeans.size() > 0) {
                            for (String bn : preDependNodeBeans) {
                                System.out.println("preDependNodeBeans添加进去的为：" + bn);
                                newDependsOn.add(bn);
                            }
                        }
                    }
                }
                deleteDepend.getDependsOn().remove(delete.getName());
                deleteDepend.getDependsOn().addAll(newDependsOn);

            }
            currentNodeBeans.remove(delete);
        }
        // 把新设置好的节点列表放进flow
        nodeBean.setNodes(currentNodeBeans);

        nodeBean.setName(getFlowName(flowFile));
        nodeBean.setType(Constants.FLOW_NODE_TYPE);

        return nodeBean;
    }


    public boolean validate(final NodeBean nodeBean) {
        final Set<String> nodeNames = new HashSet<>();
        for (final NodeBean n : nodeBean.getNodes()) {
            if (!nodeNames.add(n.getName())) {
                // Duplicate jobs
                return false;
            }
        }

        for (final NodeBean n : nodeBean.getNodes()) {
            if (n.getDependsOn() != null && !nodeNames.containsAll(n.getDependsOn())) {
                // Undefined reference to dependent job
                return false;
            }
        }

        if (nodeNames.contains(Constants.ROOT_NODE_IDENTIFIER)) {
            // ROOT is reserved as a special value in runtimeProperties
            return false;
        }

        return true;
    }

    public AzkabanNode toAzkabanNode(final NodeBean nodeBean) {
        if (nodeBean.getType().equals(Constants.FLOW_NODE_TYPE)) {
            return new AzkabanFlow.AzkabanFlowBuilder()
                    .name(nodeBean.getName())
                    .props(nodeBean.getProps())
                    .condition(nodeBean.getCondition())
                    .dependsOn(nodeBean.getDependsOn())
                    .nodes(nodeBean.getNodes().stream().map(this::toAzkabanNode).collect(Collectors.toList()))
                    .flowTrigger(toFlowTrigger(nodeBean.getTrigger()))
                    .build();
        } else {
            return new AzkabanJob.AzkabanJobBuilder()
                    .name(nodeBean.getName())
                    .props(nodeBean.getProps())
                    .condition(nodeBean.getCondition())
                    .type(nodeBean.getType())
                    .dependsOn(nodeBean.getDependsOn())
                    .build();
        }
    }

    private void validateSchedule(final FlowTriggerBean flowTriggerBean) {
        final Map<String, String> scheduleMap = flowTriggerBean.getSchedule();

        Preconditions.checkNotNull(scheduleMap, "flow trigger schedule must not be null");

        Preconditions.checkArgument(
                scheduleMap.containsKey(FlowTriggerProps.SCHEDULE_TYPE) && scheduleMap.get
                                (FlowTriggerProps.SCHEDULE_TYPE)
                        .equals(FlowTriggerProps.CRON_SCHEDULE_TYPE),
                "flow trigger schedule type must be cron");

        Preconditions
                .checkArgument(scheduleMap.containsKey(FlowTriggerProps.SCHEDULE_VALUE) && CronExpression
                                .isValidExpression(scheduleMap.get(FlowTriggerProps.SCHEDULE_VALUE)),
                        "flow trigger schedule value must be a valid cron expression");

        final String cronExpression = scheduleMap.get(FlowTriggerProps.SCHEDULE_VALUE).trim();
        final String[] cronParts = cronExpression.split("\\s+");

        Preconditions
                .checkArgument(cronParts[0].equals("0"), "interval of flow trigger schedule has to"
                        + " be larger than 1 min");

        Preconditions.checkArgument(scheduleMap.size() == 2, "flow trigger schedule must "
                + "contain type and value only");
    }

    private void validateFlowTriggerBean(final FlowTriggerBean flowTriggerBean) {
        validateSchedule(flowTriggerBean);
        validateTriggerDependencies(flowTriggerBean.getTriggerDependencies());
        validateMaxWaitMins(flowTriggerBean);
    }

    private void validateMaxWaitMins(final FlowTriggerBean flowTriggerBean) {
        Preconditions.checkArgument(flowTriggerBean.getTriggerDependencies().isEmpty() ||
                        flowTriggerBean.getMaxWaitMins() != null,
                "max wait min cannot be null unless no dependency is defined");

        if (flowTriggerBean.getMaxWaitMins() != null) {
            Preconditions.checkArgument(flowTriggerBean.getMaxWaitMins() >= Constants
                    .MIN_FLOW_TRIGGER_WAIT_TIME.toMinutes(), "max wait min must be at least " + Constants
                    .MIN_FLOW_TRIGGER_WAIT_TIME.toMinutes() + " min(s)");
        }
    }

    /**
     * check uniqueness of dependency.name
     */
    private void validateDepNameUniqueness(final List<TriggerDependencyBean> dependencies) {
        final Set<String> seen = new HashSet<>();
        for (final TriggerDependencyBean dep : dependencies) {
            // set.add() returns false when there exists duplicate
            Preconditions.checkArgument(seen.add(dep.getName()), String.format("duplicate dependency"
                    + ".name %s found, dependency.name should be unique", dep.getName()));
        }
    }

    /**
     * check uniqueness of dependency type and params
     */
    private void validateDepDefinitionUniqueness(final List<TriggerDependencyBean> dependencies) {
        for (int i = 0; i < dependencies.size(); i++) {
            for (int j = i + 1; j < dependencies.size(); j++) {
                final boolean duplicateDepDefFound =
                        dependencies.get(i).getType().equals(dependencies.get(j)
                                .getType()) && dependencies.get(i).getParams()
                                .equals(dependencies.get(j).getParams());
                Preconditions.checkArgument(!duplicateDepDefFound, String.format("duplicate dependency"
                                + "config %s found, dependency config should be unique",
                        dependencies.get(i).getName()));
            }
        }
    }

    /**
     * validate name and type are present
     */
    private void validateNameAndTypeArePresent(final List<TriggerDependencyBean> dependencies) {
        for (final TriggerDependencyBean dep : dependencies) {
            Preconditions.checkNotNull(dep.getName(), "dependency name is required");
            Preconditions.checkNotNull(dep.getType(), "dependency type is required for " + dep.getName());
        }
    }

    private void validateTriggerDependencies(final List<TriggerDependencyBean> dependencies) {
        validateNameAndTypeArePresent(dependencies);
        validateDepNameUniqueness(dependencies);
        validateDepDefinitionUniqueness(dependencies);
        validateDepType(dependencies);
    }

    private void validateDepType(final List<TriggerDependencyBean> dependencies) {
        //todo chengren311: validate dependencies are of valid dependency type
    }

    public FlowTrigger toFlowTrigger(final FlowTriggerBean flowTriggerBean) {
        if (flowTriggerBean == null) {
            return null;
        } else {
            validateFlowTriggerBean(flowTriggerBean);
            if (flowTriggerBean.getMaxWaitMins() != null
                    && flowTriggerBean.getMaxWaitMins() > Constants.DEFAULT_FLOW_TRIGGER_MAX_WAIT_TIME
                    .toMinutes()) {
                flowTriggerBean.setMaxWaitMins(Constants.DEFAULT_FLOW_TRIGGER_MAX_WAIT_TIME.toMinutes());
            }

            final Duration duration = flowTriggerBean.getMaxWaitMins() == null ? null : Duration
                    .ofMinutes(flowTriggerBean.getMaxWaitMins());

            return new FlowTrigger(
                    new CronSchedule(flowTriggerBean.getSchedule().get(FlowTriggerProps.SCHEDULE_VALUE)),
                    flowTriggerBean.getTriggerDependencies().stream()
                            .map(d -> new FlowTriggerDependency(d.getName(), d.getType(), d.getParams()))
                            .collect(Collectors.toList()), duration);
        }
    }

    public String getFlowName(final File flowFile) {
        checkArgument(flowFile != null && flowFile.exists());
        checkArgument(flowFile.getName().endsWith(Constants.FLOW_FILE_SUFFIX));

        return Files.getNameWithoutExtension(flowFile.getName());
    }
}
