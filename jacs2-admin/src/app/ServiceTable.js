import React, { Component } from 'react';
import { connect } from 'react-redux';
import Table from 'grommet/components/Table';
import TableHeader from 'grommet/components/TableHeader';
import Section from 'grommet/components/Section';
import Search from 'grommet/components/Search';
import ServiceRowItem from './ServiceRowItem';
import { fetchServices } from './actions'


class ServiceTable extends Component {
  componentDidMount() {
      this.props.fetchServices('http://jacs2.int.janelia.org:9000');
  }

  render() {
      return (
        <Section>
            <Search/>
        <Table full={true} scrollable={false} selectable={true}>
          <TableHeader labels={['Name', 'Actions']} />
          <tbody>
               {this.props.services.map((service) =>
                  <ServiceRowItem key={service.serviceName} service={service}/>
              )} 
          </tbody>
      </Table>
      </Section>

      );
  }
}

const mapStateToProps = (state) => {
    return {
        services: state.servicesRegistry.services
    };
};
const mapDispatchToProps = (dispatch) => {
    return {
        fetchServices: (url) => dispatch(fetchServices(url))
    };
};

export default connect(mapStateToProps, mapDispatchToProps)(ServiceTable);